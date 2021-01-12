// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.openmrs.analytics;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.gson.Gson;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.openmrs.analytics.model.EventConfiguration;
import org.openmrs.analytics.model.GeneralConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFetchUtil {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcFetchUtil.class);
	
	private ComboPooledDataSource comboPooledDataSource;
	
	JdbcFetchUtil(String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword, int dbcMaxPoolSize)
	        throws IOException, SQLException, PropertyVetoException {
		comboPooledDataSource = new ComboPooledDataSource();
		comboPooledDataSource.setDriverClass(jdbcDriverClass);
		comboPooledDataSource.setJdbcUrl(jdbcUrl);
		comboPooledDataSource.setUser(dbUser);
		comboPooledDataSource.setPassword(dbPassword);
		comboPooledDataSource.setMaxPoolSize(dbcMaxPoolSize);
		comboPooledDataSource.setInitialPoolSize(10); // TODO: make this configurable if issues arises
	}
	
	public static class FetchUuIds extends PTransform<PCollection<KV<String, Integer>>, PCollection<String>> {
		
		private final String tableName;
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchUuIds(String tableName, JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.tableName = tableName;
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<String> expand(PCollection<KV<String, Integer>> idRanges) {
			String tableId = this.tableName + "_id";
			return idRanges.apply(String.format("Read UUIDs from %s", this.tableName),
			    JdbcIO.<KV<String, Integer>, String> readAll().withDataSourceConfiguration(this.dataSourceConfig)
			            .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String, Integer>>() {
				            
				            @Override
				            public void setParameters(KV<String, Integer> element, PreparedStatement preparedStatement)
				                    throws Exception {
					            String[] range = Objects.requireNonNull(element.getKey()).split(",");
					            preparedStatement.setInt(1, Integer.parseInt(range[0]));
					            preparedStatement.setInt(2, Integer.parseInt(range[1]));
				            }
			            }).withOutputParallelization(false)
			            .withQuery(
			                String.format("select uuid from %s where %s >= ? and %s < ?", this.tableName, tableId, tableId))
			            .withRowMapper(new JdbcIO.RowMapper<String>() {
				            
				            @Override
				            public String mapRow(ResultSet resultSet) throws Exception {
					            return resultSet.getString("uuid");
				            }
			            }).withCoder(NullableCoder.of(StringUtf8Coder.of())));
		}
	}
	
	/**
	 * batch together request using batchSize and generate segment descriptors `_id?<uuid>,<uuid>..`
	 */
	public static class CreateSearchSegments extends PTransform<PCollection<String>, PCollection<SearchSegmentDescriptor>> {
		
		private final String baseBundleUrl;
		
		private final String resourceType;
		
		private final int batchSize;
		
		public CreateSearchSegments(String resourceType, String baseBundleUrl, int batchSize) {
			this.baseBundleUrl = baseBundleUrl;
			this.batchSize = batchSize;
			this.resourceType = resourceType;
		}
		
		@Override
		public PCollection<SearchSegmentDescriptor> expand(PCollection<String> uuIds) {
			return uuIds
			        // create KV required by GroupIntoBatches
			        .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
				        
				        @ProcessElement
				        public void processElement(ProcessContext c) {
					        if (c.element() != null) {
						        c.output(KV.of(resourceType, c.element()));
					        }
				        }
			        }))
			        .apply(String.format("GroupIntoBatches of %s", this.batchSize),
			            GroupIntoBatches.<String, String> ofSize(this.batchSize))
			        .apply("Generate Segments", ParDo.of(new DoFn<KV<String, Iterable<String>>, SearchSegmentDescriptor>() {
				        
				        @ProcessElement
				        public void processElement(@Element KV<String, Iterable<String>> element,
				                OutputReceiver<SearchSegmentDescriptor> r) {
					        List<String> uuIds = new ArrayList<String>();
					        element.getValue().forEach(uuIds::add);
					        r.output(SearchSegmentDescriptor.create(
					            String.format("%s?_id=%s", baseBundleUrl, String.join(",", uuIds)), uuIds.size()));
					        
				        }
			        }));
		}
	}
	
	public ResultSet fetchMaxId(String tableName) throws SQLException {
		String tableId = tableName + "_id";
		Connection con = this.comboPooledDataSource.getConnection();
		Statement statement = con.createStatement();
		ResultSet resultSet = statement.executeQuery(String.format("SELECT MAX(`%s`) from %s", tableId, tableName));
		return resultSet;
	}
	
	public JdbcIO.DataSourceConfiguration getJdbcConfig() {
		return JdbcIO.DataSourceConfiguration.create(this.comboPooledDataSource);
	}
	
	public Map<String, Integer> createIdRanges(int count, int rangeSize) {
		int ranges = count / rangeSize;
		Map<String, Integer> rangeMap = new HashMap<String, Integer>();
		for (int i = 0; i < ranges; i++) {
			int rangeFrom = i * rangeSize;
			int rangeTo = (i + 1) * rangeSize;
			String range = String.format("%s,%s", rangeFrom, rangeTo);
			rangeMap.put(range, rangeFrom);
		}
		if (count > ranges * rangeSize) {
			int rangeFrom = ranges * rangeSize;
			int rangeTo = ranges * rangeSize + count % rangeSize;
			String range = String.format("%s,%s", rangeFrom, rangeTo);
			rangeMap.put(range, rangeFrom);
		}
		return rangeMap;
	}
	
	public Map<String, String> createFhirReverseMap(String searchString, String tableFhirMapPath) throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(tableFhirMapPath);
		try (Reader reader = Files.newBufferedReader(pathToFile.toAbsolutePath(), StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfiguration = gson.fromJson(reader, GeneralConfiguration.class);
			LinkedHashMap<String, EventConfiguration> tableToFhirMap = generalConfiguration.getEventConfigurations();
			String[] searchList = searchString.split(",");
			Map<String, String> reverseMap = new LinkedHashMap<String, String>();
			for (Map.Entry<String, EventConfiguration> entry : tableToFhirMap.entrySet()) {
				LinkedHashMap<String, String> linkTemplate = entry.getValue().getLinkTemplates();
				for (String search : searchList) {
					if (linkTemplate.containsKey("fhir") && linkTemplate.get("fhir") != null) {
						String[] resourceName = linkTemplate.get("fhir").split("/");
						if (resourceName.length >= 1 && resourceName[1].equals(search)) {
							reverseMap.put(entry.getValue().getParentTable(), resourceName[1]);
						}
					}
				}
			}
			if (reverseMap.size() < searchList.length) {
				log.error("Some of the passed FHIR resources are not mapped to any table, please check the config");
			}
			return reverseMap;
		}
	}
}
