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
import java.sql.SQLException;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the common functionality for all Fns that need to fetch FHIR resources and convert them
 * to Avro and JSON records. The non-abstract sub-classes should implement `ProcessElement` using
 * `processBundle` auxiliary method. Note the code reuse pattern that we really need here is
 * composition (not inheritance) but because of Beam complexities (e.g., certain work need to be
 * done during `setup()` where PipelienOptions not available) we use inheritance.
 *
 * @param <T> The type of the elements of the input PCollection.
 */
abstract class FetchSearchPageFn<T> extends DoFn<T, KV<String, Integer>> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchSearchPageFn.class);
	
	private final Counter numFetchedResources;
	
	private final Counter totalFetchTimeMillis;
	
	private final Counter totalGenerateTimeMillis;
	
	private final Counter totalPushTimeMillis;
	
	private final String sourceUrl;
	
	private final String sourceUser;
	
	private final String sourcePw;
	
	protected final String sinkPath;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	protected final String stageIdentifier;
	
	protected final String parquetFile;
	
	private final int secondsToFlush;
	
	private final int rowGroupSize;
	
	protected final String sinkDbUrl;
	
	private final String sinkDbUsername;
	
	private final String sinkDbPassword;
	
	private final String sinkDbTableName;
	
	private final boolean useSingleSinkDbTable;
	
	private final String jdbcDriverClass;
	
	private final int initialPoolSize;
	
	private final int maxPoolSize;
	
	@VisibleForTesting
	protected ParquetUtil parquetUtil;
	
	protected OpenmrsUtil openmrsUtil;
	
	protected FhirSearchUtil fhirSearchUtil;
	
	protected FhirStoreUtil fhirStoreUtil;
	
	protected JdbcResourceWriter jdbcWriter;
	
	protected IParser parser;
	
	private static JdbcConnectionUtil jdbcConnectionUtil = null;
	
	// This is to enforce the Singleton pattern for JdbcConnectionUtil used by all workers running
	// this DoFn on the same VM; hence sharing connections.
	private static synchronized JdbcConnectionUtil getJdbcConnectionUtil(String jdbcDriverClass, String jdbcUrl,
	        String dbUser, String dbPassword, int initialPoolSize, int jdbcMaxPoolSize) throws PropertyVetoException {
		if (jdbcConnectionUtil == null) {
			jdbcConnectionUtil = new JdbcConnectionUtil(jdbcDriverClass, jdbcUrl, dbUser, dbPassword, initialPoolSize,
			        jdbcMaxPoolSize);
		}
		return jdbcConnectionUtil;
	}
	
	FetchSearchPageFn(FhirEtlOptions options, String stageIdentifier) {
		this.sinkPath = options.getFhirSinkPath();
		this.sinkUsername = options.getSinkUserName();
		this.sinkPassword = options.getSinkPassword();
		this.sourceUrl = options.getFhirServerUrl();
		this.sourceUser = options.getFhirServerUserName();
		this.sourcePw = options.getFhirServerPassword();
		this.stageIdentifier = stageIdentifier;
		this.parquetFile = options.getOutputParquetPath();
		this.secondsToFlush = options.getSecondsToFlushParquetFiles();
		this.rowGroupSize = options.getRowGroupSizeForParquetFiles();
		this.sinkDbUrl = options.getSinkDbUrl();
		this.sinkDbTableName = options.getSinkDbTablePrefix();
		this.sinkDbUsername = options.getSinkDbUsername();
		this.sinkDbPassword = options.getSinkDbPassword();
		this.useSingleSinkDbTable = options.getUseSingleSinkTable();
		this.initialPoolSize = options.getJdbcInitialPoolSize();
		this.maxPoolSize = options.getJdbcMaxPoolSize();
		// We are assuming that the potential source and sink DBs are the same type.
		this.jdbcDriverClass = options.getJdbcDriverClass();
		this.numFetchedResources = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + stageIdentifier);
		this.totalFetchTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalFetchTimeMillis_" + stageIdentifier);
		this.totalGenerateTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE,
		    "totalGenerateTimeMillis_" + stageIdentifier);
		this.totalPushTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + stageIdentifier);
	}
	
	@Setup
	public void setup() throws SQLException, PropertyVetoException {
		FhirContext fhirContext = FhirContext.forR4();
		fhirContext.getRestfulClientFactory().setSocketTimeout(20000);
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		openmrsUtil = new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
		fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
		parquetUtil = new ParquetUtil(parquetFile, secondsToFlush, rowGroupSize, stageIdentifier + "_");
		if (!sinkDbUrl.isEmpty()) {
			jdbcWriter = new JdbcResourceWriter(getJdbcConnectionUtil(jdbcDriverClass, sinkDbUrl, sinkDbUsername,
			    sinkDbPassword, initialPoolSize, maxPoolSize).getDataSource(), sinkDbTableName, useSingleSinkDbTable,
			        fhirContext);
		}
		parser = fhirContext.newJsonParser();
	}
	
	@Teardown
	public void teardown() throws IOException {
		parquetUtil.closeAllWriters();
	}
	
	protected void addFetchTime(long millis) {
		totalFetchTimeMillis.inc(millis);
	}
	
	protected void processBundle(Bundle bundle) throws IOException, SQLException {
		if (bundle != null && bundle.getEntry() != null) {
			numFetchedResources.inc(bundle.getEntry().size());
			if (!parquetFile.isEmpty()) {
				long startTime = System.currentTimeMillis();
				parquetUtil.writeRecords(bundle);
				totalGenerateTimeMillis.inc(System.currentTimeMillis() - startTime);
			}
			if (!this.sinkPath.isEmpty()) {
				long pushStartTime = System.currentTimeMillis();
				fhirStoreUtil.uploadBundle(bundle);
				totalPushTimeMillis.inc(System.currentTimeMillis() - pushStartTime);
			}
			if (!this.sinkDbUrl.isEmpty()) {
				if (bundle.getTotal() == 0) {
					return;
				}
				for (BundleEntryComponent entry : bundle.getEntry()) {
					Resource resource = entry.getResource();
					jdbcWriter.writeResource(resource);
				}
			}
		}
	}
	
}
