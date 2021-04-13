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

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
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
	
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	JdbcFetchUtil(JdbcConnectionUtil jdbcConnectionUtil) {
		this.jdbcConnectionUtil = jdbcConnectionUtil;
	}
	
	public static class FetchUuids extends PTransform<PCollection<KV<Integer, Integer>>, PCollection<String>> {
		
		private final String tableName;
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchUuids(String tableName, JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.tableName = tableName;
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<String> expand(PCollection<KV<Integer, Integer>> idRanges) {
			String tableId = this.tableName + "_id";
			return idRanges.apply(String.format("Read UUIDs from %s", this.tableName),
			    JdbcIO.<KV<Integer, Integer>, String> readAll().withDataSourceConfiguration(this.dataSourceConfig)
			            .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, Integer>>() {
				            
				            @Override
				            public void setParameters(KV<Integer, Integer> element, PreparedStatement preparedStatement)
				                    throws Exception {
					            preparedStatement.setInt(1, element.getKey());
					            preparedStatement.setInt(2, element.getValue());
				            }
			            })
			            .withQuery(
			                String.format("SELECT uuid FROM %s WHERE %s >= ? AND %s <= ?", this.tableName, tableId, tableId))
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
		public PCollection<SearchSegmentDescriptor> expand(PCollection<String> resourceUuids) {
			return resourceUuids
			        // create KV required by GroupIntoBatches
			        .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
				        
				        @ProcessElement
				        public void processElement(@Element String element, OutputReceiver<KV<String, String>> r) {
					        if (element != null) {
						        r.output(KV.of(resourceType, element));
					        }
				        }
			        }))
			        .apply(String.format("GroupIntoBatches of %s", this.batchSize),
			            GroupIntoBatches.<String, String> ofSize(this.batchSize))
			        .apply("Generate Segments", ParDo.of(new DoFn<KV<String, Iterable<String>>, SearchSegmentDescriptor>() {
				        
				        @ProcessElement
				        public void processElement(@Element KV<String, Iterable<String>> element,
				                OutputReceiver<SearchSegmentDescriptor> r) {
					        List<String> uuids = new ArrayList<String>();
					        element.getValue().forEach(uuids::add);
					        r.output(SearchSegmentDescriptor
					                .create(String.format("%s?_id=%s", baseBundleUrl, String.join(",", uuids)), uuids.size() //please note that, setting count here has no effect.
					        ));
					        
				        }
			        }));
		}
	}
	
	public Integer fetchMaxId(String tableName) throws SQLException {
		String tableId = tableName + "_id";
		Statement statement = jdbcConnectionUtil.createStatement();
		ResultSet resultSet = statement
		        .executeQuery(String.format("SELECT MAX(`%s`) as max_id FROM %s", tableId, tableName));
		resultSet.first();
		int maxId = resultSet.getInt("max_id");
		resultSet.close();
		statement.close();
		return maxId;
	}
	
	public JdbcIO.DataSourceConfiguration getJdbcConfig() {
		return JdbcIO.DataSourceConfiguration.create(this.jdbcConnectionUtil.getConnectionObject());
	}
	
	public Map<Integer, Integer> createIdRanges(int count, int rangeSize) {
		int ranges = count / rangeSize;
		Map<Integer, Integer> rangeMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < ranges; i++) {
			int rangeFrom = (i * rangeSize) + 1;
			int rangeTo = (i + 1) * rangeSize;
			rangeMap.put(rangeFrom, rangeTo);
		}
		if (count > ranges * rangeSize) {
			int rangeFrom = ranges * rangeSize;
			int rangeTo = ranges * rangeSize + count % rangeSize;
			rangeMap.put(rangeFrom, rangeTo);
		}
		return rangeMap;
	}
	/**
	 * creates a Map<String, List<String>> of table names to a list of coresponding Resources passed to the searchList argument
	 */
	public Map<String, List<String>> createFhirReverseMap(String searchString, String tableFhirMapPath) throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(tableFhirMapPath);
		try (Reader reader = Files.newBufferedReader(pathToFile.toAbsolutePath(), StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfiguration = gson.fromJson(reader, GeneralConfiguration.class);
			Map<String, EventConfiguration> tableToFhirMap = generalConfiguration.getEventConfigurations();
			String[] searchList = searchString.split(",");
			Map<String, List<String>> reverseMap = new HashMap<String, List<String>>();
			for (Map.Entry<String, EventConfiguration> entry : tableToFhirMap.entrySet()) {
				Map<String, String> linkTemplate = entry.getValue().getLinkTemplates();
				for (String search : searchList) {
					if (linkTemplate.containsKey("fhir") && linkTemplate.get("fhir") != null) {
						String[] resourceName = linkTemplate.get("fhir").split("/");
						List<String> resources;
						if (resourceName.length >= 1 && resourceName[1].equals(search)) {
							if (reverseMap.containsKey(entry.getValue().getParentTable())) {
								resources = reverseMap.get(entry.getValue().getParentTable());
								resources.add(resourceName[1]);
							} else {
								resources = new ArrayList<String>();
								resources.add(resourceName[1]);
								reverseMap.put(entry.getValue().getParentTable(), resources);
							}
							
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
