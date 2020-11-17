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

import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.dstu3.model.Resource;
import org.openmrs.analytics.model.EventConfiguration;
import org.openmrs.analytics.model.GeneralConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFhirMode {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcFhirMode.class);
	
	JdbcFhirMode() {
	}
	
	public JdbcIO.DataSourceConfiguration getJdbcConfig(FhirEtl.FhirEtlOptions options) throws PropertyVetoException {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		dataSource.setDriverClass(options.getJdbcDriverClass());
		dataSource.setJdbcUrl(options.getJdbcUrl());
		dataSource.setUser(options.getDbUser());
		dataSource.setPassword(options.getDbPassword());
		dataSource.setMaxPoolSize(10); // TODO make this configurable
		dataSource.setInitialPoolSize(6); // TODO make this configurable
		return JdbcIO.DataSourceConfiguration.create(dataSource);
	}
	
	public static PCollection<KV<String, Iterable<Integer>>> createChunkRanges(String tableName, int fetchSize, Pipeline p,
	        JdbcIO.DataSourceConfiguration config) {
		String tableId = tableName + "_id";
		return p.apply(String.format("Read Max table Index Column for: %s", tableName),
		    JdbcIO.<String> read().withDataSourceConfiguration(config)
		            .withQuery(String.format("SELECT MAX(`%s`) from %s", tableId, tableName))
		            .withRowMapper(new JdbcIO.RowMapper<String>() {
			            
			            @Override
			            public String mapRow(ResultSet resultSet) throws Exception {
				            return resultSet.getString(1);
			            }
		            }).withOutputParallelization(false).withCoder(StringUtf8Coder.of()))
		        .apply("Distribute", ParDo.of(new DoFn<String, KV<String, Integer>>() {
			            
			        @ProcessElement
			        public void processElement(ProcessContext c) {
				        int count = Integer.parseInt(c.element());
				        int ranges = (int) (count / fetchSize);
				        for (int i = 0; i < ranges; i++) {
					        int rangeFrom = i * fetchSize;
					        int rangeTo = (i + 1) * fetchSize;
					        String range = String.format("%s,%s", rangeFrom, rangeTo);
					        c.output(KV.of(range, rangeFrom));
				        }
				        if (count > ranges * fetchSize) {
					        int rangeFrom = ranges * fetchSize;
					        int rangeTo = ranges * fetchSize + count % fetchSize;
					        String range = String.format("%s,%s", rangeFrom, rangeTo);
					        c.output(KV.of(range, rangeFrom));
				        }
			        }
		        })).apply("Group By", GroupByKey.create());
	}
	
	public static PCollection<String> fetchUuids(String tableName, int fetchSize,
	        PCollection<KV<String, Iterable<Integer>>> ranges, JdbcIO.DataSourceConfiguration config) {
		String tableId = tableName + "_id";
		return ranges.apply(String.format("Read UUIDs from %s", tableName),
		    JdbcIO.<KV<String, Iterable<Integer>>, String> readAll().withDataSourceConfiguration(config)
		            .withFetchSize(fetchSize).withCoder(StringUtf8Coder.of())
		            .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<String, Iterable<Integer>>>() {
			            
			            @Override
			            
			            public void setParameters(KV<String, Iterable<Integer>> element, PreparedStatement preparedStatement)
			                    throws Exception {
				            String[] range = element.getKey().split(",");
				            preparedStatement.setInt(1, Integer.parseInt(range[0]));
				            preparedStatement.setInt(2, Integer.parseInt(range[1]));
			            }
		            }).withOutputParallelization(false)
		            .withQuery(String.format("select uuid from %s where %s >= ? and %s < ?", tableName, tableId, tableId))
		            .withRowMapper((JdbcIO.RowMapper<String>) resultSet -> resultSet.getString("uuid")));
	}
	
	LinkedHashMap<String, String> createFhirReverseMap(FhirEtl.FhirEtlOptions options) throws IOException {
		LinkedHashMap<String, EventConfiguration> tableToFhirMap = this.getTableToFhirConfig(options.getTableFhirMapPath());
		LinkedHashMap<String, String> reversHashMap = new LinkedHashMap<String, String>();
		for (String search : options.getSearchList().split(",")) {
			for (Map.Entry<String, EventConfiguration> entry : tableToFhirMap.entrySet()) {
				LinkedHashMap<String, String> linkTemplate = entry.getValue().getLinkTemplates();
				if (linkTemplate.containsKey("fhir") && linkTemplate.get("fhir") != null) {
					String[] resourceName = linkTemplate.get("fhir").split("/");
					if (resourceName.length >= 1 && resourceName[1].equals(search))
						reversHashMap.put(entry.getValue().getUuidTable(), resourceName[1]);
				}
			}
			
		}
		return (reversHashMap);
	}
	
	LinkedHashMap<String, EventConfiguration> getTableToFhirConfig(String filePathName) throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(filePathName);
		try (Reader reader = Files.newBufferedReader(pathToFile.toAbsolutePath(), StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfiguration = gson.fromJson(reader, GeneralConfiguration.class);
			return generalConfiguration.getEventConfigurations();
		}
		
	}
	
	static class FhirSink extends DoFn<String, GenericRecord> {
		
		private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);
		
		private String sourceUrl;
		
		private String fhirUrl;
		
		private String sourceUser;
		
		private String sourcePw;
		
		private String sinkPath;
		
		private String sinkUsername;
		
		private String sinkPassword;
		
		private String parquetFile;
		
		private ParquetUtil parquetUtil;
		
		private FhirContext fhirContext;
		
		private FhirSearchUtil fhirSearchUtil;
		
		private FhirStoreUtil fhirStoreUtil;
		
		private OpenmrsUtil openmrsUtil;
		
		FhirSink(String sinkPath, String sinkUsername, String sinkPassword, String sourceUrl, String parquetFile,
		    String sourceUser, String sourcePw, String fhirUrl) {
			this.sinkPath = sinkPath;
			this.sinkUsername = sinkUsername;
			this.sinkPassword = sinkPassword;
			this.sourceUrl = sourceUrl;
			this.sourceUser = sourceUser;
			this.sourcePw = sourcePw;
			this.parquetFile = parquetFile;
			this.fhirUrl = fhirUrl;
		}
		
		@Setup
		public void Setup() {
			fhirContext = FhirContext.forDstu3();
			fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
			    fhirContext.getRestfulClientFactory());
			openmrsUtil = new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
			fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
			parquetUtil = new ParquetUtil(fhirContext);
		}
		
		@ProcessElement
		public void ProcessElement(@Element String uuid, OutputReceiver<GenericRecord> out) {
			String fhirUri = fhirUrl.replace("{uuid}", uuid);
			log.info("Fetching FHIR resource at " + fhirUri);
			Resource resource = openmrsUtil.fetchFhirResource(fhirUri.replace("{uuid}", uuid));
			
			if (parquetFile.isEmpty()) {
				log.info("Saving to FHIR store: " + fhirUri);
				fhirStoreUtil.uploadResource(resource);
			} else {
				if (resource != null)
					out.output(parquetUtil.convertToAvro(resource));
			}
		}
	}
	
}
