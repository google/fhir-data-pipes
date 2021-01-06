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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.google.gson.Gson;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.openmrs.analytics.model.EventConfiguration;
import org.openmrs.analytics.model.GeneralConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFhirMode {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcFhirMode.class);
	
	public static class FetchMaxId extends PTransform<PBegin, PCollection<Integer>> {
		
		private final String tableName;
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchMaxId(String tableName, JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.tableName = tableName;
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<Integer> expand(PBegin input) {
			String tableId = tableName + "_id";
			return input.apply(String.format("Read Max table Index Column for: %s", tableName),
			    JdbcIO.<Integer> read().withDataSourceConfiguration(dataSourceConfig)
			            .withQuery(String.format("SELECT MAX(`%s`) from %s", tableId, tableName))
			            .withRowMapper(new JdbcIO.RowMapper<Integer>() {
				            
				            @Override
				            public Integer mapRow(ResultSet resultSet) throws Exception {
					            return Integer.parseInt(resultSet.getString(1));
				            }
			            }).withOutputParallelization(false).withCoder(VarIntCoder.of()));
			
			// Uuid might be missing in child table, to solve this issue we are using UuidTable field mappings in json config
			// TODO: A better solution is to Use join clause to get parent table, See comments left here:
			//  https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/46#issuecomment-728979138
			
		}
	}
	
	public static class CreateIdRanges extends PTransform<PCollection<Integer>, PCollection<KV<String, Iterable<Integer>>>> {
		
		private final int batchSize;
		
		public CreateIdRanges(int batchSize) {
			this.batchSize = batchSize;
		}
		
		@Override
		public PCollection<KV<String, Iterable<Integer>>> expand(PCollection<Integer> ids) {
			return ids.apply("Distribute", ParDo.of(new DoFn<Integer, KV<String, Integer>>() {
				
				@ProcessElement
				public void processElement(ProcessContext c) {
					int count = c.element();
					int ranges = count / batchSize;
					for (int i = 0; i < ranges; i++) {
						int rangeFrom = i * batchSize;
						int rangeTo = (i + 1) * batchSize;
						String range = String.format("%s,%s", rangeFrom, rangeTo);
						c.output(KV.of(range, rangeFrom));
					}
					if (count > ranges * batchSize) {
						int rangeFrom = ranges * batchSize;
						int rangeTo = ranges * batchSize + count % batchSize;
						String range = String.format("%s,%s", rangeFrom, rangeTo);
						c.output(KV.of(range, rangeFrom));
					}
				}
			})).apply("Group By", GroupByKey.create()); //necessary for returning an Iterable of ranges
		}
	}
	
	public static class FetchUuIds extends PTransform<PCollection<KV<String, Iterable<Integer>>>, PCollection<String>> {
		
		private final String tableName;
		
		private final int batchSize;
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchUuIds(String tableName, int batchSize, JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.tableName = tableName;
			this.batchSize = batchSize;
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<String> expand(PCollection<KV<String, Iterable<Integer>>> idRanges) {
			String tableId = this.tableName + "_id";
			return idRanges.apply(String.format("Read UUIDs from %s", this.tableName),
			    JdbcIO.<KV<String, Iterable<Integer>>, String> readAll().withDataSourceConfiguration(this.dataSourceConfig)
			            .withFetchSize(this.batchSize)
			            .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String, Iterable<Integer>>>() {
				            
				            @Override
				            public void setParameters(KV<String, Iterable<Integer>> element,
				                    PreparedStatement preparedStatement) throws Exception {
					            String[] range = Objects.requireNonNull(element.getKey()).split(",");
					            preparedStatement.setInt(1, Integer.parseInt(range[0]));
					            preparedStatement.setInt(2, Integer.parseInt(range[1]));
				            }
			            }).withOutputParallelization(false)
			            .withQuery(String.format(
			                "select GROUP_CONCAT(uuid SEPARATOR ',') as uuids from %s where %s >= ? and %s < ?",
			                this.tableName, tableId, tableId))
			            .withRowMapper(new JdbcIO.RowMapper<String>() {
				            
				            @Override
				            public String mapRow(ResultSet resultSet) throws Exception {
					            return resultSet.getString("uuids");
				            }
			            }).withCoder(NullableCoder.of(StringUtf8Coder.of())));
		}
	}
	
	public static class CreateSearchSegments extends PTransform<PCollection<String>, PCollection<SearchSegmentDescriptor>> {
		
		private final String baseBundleUrl;
		
		private final int parallelRequests;
		
		public CreateSearchSegments(String baseBundleUrl, int parallelRequests) {
			this.baseBundleUrl = baseBundleUrl;
			this.parallelRequests = parallelRequests;
		}
		
		@Override
		public PCollection<SearchSegmentDescriptor> expand(PCollection<String> uuIds) {
			return uuIds
			        // create KV required by GroupIntoBatches
			        .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
				        
				        @ProcessElement
				        public void processElement(ProcessContext c) {
					        if (c.element() != null) {
						        c.output(KV.of(c.element().split(",").length,
						            String.format("%s?_id=%s", baseBundleUrl, c.element())));
					        }
				        }
			        }))
			        // batchSize is used here instead of jdbcFetchSize to limit the number of  `_id` passed per request
			        // TODO: find out why passing batchSize > 1000 sometimes results in OpenMRS internal server error
			        .apply(String.format("GroupIntoBatches of %s", this.parallelRequests),
			            GroupIntoBatches.<Integer, String> ofSize(this.parallelRequests))
			        .apply("Generate Segments", ParDo.of(new DoFn<KV<Integer, Iterable<String>>, SearchSegmentDescriptor>() {
				        
				        @ProcessElement
				        public void processElement(@Element KV<Integer, Iterable<String>> element,
				                OutputReceiver<SearchSegmentDescriptor> r) {
					        element.getValue().forEach(
					            bundleUrl -> r.output(SearchSegmentDescriptor.create(bundleUrl, element.getKey())));
					        
				        }
			        }));
		}
	}
	
	public JdbcIO.DataSourceConfiguration getJdbcConfig(String jdbcDriverClass, String jdbcUrl, String dbUser,
	        String dbPassword, int dbcMaxPoolSize) throws PropertyVetoException {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		dataSource.setDriverClass(jdbcDriverClass);
		dataSource.setJdbcUrl(jdbcUrl);
		dataSource.setUser(dbUser);
		dataSource.setPassword(dbPassword);
		dataSource.setMaxPoolSize(dbcMaxPoolSize);
		dataSource.setInitialPoolSize(10); // TODO: make this configurable if issues arises
		return JdbcIO.DataSourceConfiguration.create(dataSource);
	}
	
	public LinkedHashMap<String, String> createFhirReverseMap(String searchString, String tableFhirMapPath)
	        throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(tableFhirMapPath);
		try (Reader reader = Files.newBufferedReader(pathToFile.toAbsolutePath(), StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfiguration = gson.fromJson(reader, GeneralConfiguration.class);
			LinkedHashMap<String, EventConfiguration> tableToFhirMap = generalConfiguration.getEventConfigurations();
			String[] searchList = searchString.split(",");
			LinkedHashMap<String, String> reversHashMap = new LinkedHashMap<String, String>();
			for (Map.Entry<String, EventConfiguration> entry : tableToFhirMap.entrySet()) {
				LinkedHashMap<String, String> linkTemplate = entry.getValue().getLinkTemplates();
				for (String search : searchList) {
					if (linkTemplate.containsKey("fhir") && linkTemplate.get("fhir") != null) {
						String[] resourceName = linkTemplate.get("fhir").split("/");
						if (resourceName.length >= 1 && resourceName[1].equals(search)) {
							reversHashMap.put(entry.getValue().getUuidTable(), resourceName[1]);
						}
					}
				}
			}
			if (reversHashMap.size() < searchList.length) {
				log.error("Some of the passed FHIR resources are not mapped to any table, please check the config");
			}
			return reversHashMap;
		}
	}
}
