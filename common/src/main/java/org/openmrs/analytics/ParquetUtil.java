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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.avro.AvroConverter;
import com.google.common.base.Preconditions;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetUtil {
	
	private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);
	
	private final FhirContext fhirContext;
	
	private final Map<String, AvroConverter> converterMap;
	
	private final Map<String, ParquetWriter<GenericRecord>> writerMap;
	
	private String parquetFilePath;
	
	/**
	 * This is to fix the logical type conversions for BigDecimal. This should be called once before any
	 * FHIR resource conversion to Avro.
	 */
	public static void initializeAvroConverters() {
		// For more context on the next two conversions, see this thread: https://bit.ly/3iE4rwS
		// Add BigDecimal conversion to the singleton instance to fix "Unknown datum type" Avro exception.
		GenericData.get().addLogicalTypeConversion(new DecimalConversion());
		// This is for a similar error in the ParquetWriter.write which uses SpecificData.get() as its model.
		SpecificData.get().addLogicalTypeConversion(new DecimalConversion());
	}
	
	public String getParquetPath() {
		return this.parquetFilePath;
	}
	
	ParquetUtil(FhirContext fhirContext, String parquetFilePath) {
		this.fhirContext = fhirContext;
		this.converterMap = new HashMap<>();
		this.writerMap = new HashMap<>();
		this.parquetFilePath = parquetFilePath;
	}
	
	synchronized private AvroConverter getConverter(String resourceType) {
		if (!converterMap.containsKey(resourceType)) {
			AvroConverter converter = AvroConverter.forResource(fhirContext, resourceType);
			converterMap.put(resourceType, converter);
		}
		return converterMap.get(resourceType);
	}
	
	/**
	 * This is to fetch a writer for a given `resourceType`; internally it only once create a writer for
	 * a `resourceType` and on subsequent calls, return the same object. The base path comes from
	 * `file.parquetPath` system property. NOTE: This should be removed once we move the streaming
	 * pipeline to Beam; the I/O should be left to Beam similar to the batch mode.
	 */
	synchronized public ParquetWriter<GenericRecord> getWriter(String resourceType) throws IOException {
		Preconditions.checkNotNull(getParquetPath());
		if (!writerMap.containsKey(resourceType)) {
			AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
			        .builder(new Path(getParquetPath() + resourceType));
			// TODO adjust parquet file parameters for our needs or make them configurable.
			ParquetWriter<GenericRecord> writer = builder.withSchema(getResourceSchema(resourceType)).build();
			writerMap.put(resourceType, writer);
		}
		return writerMap.get(resourceType);
	}
	
	synchronized public void closeAllWriters() {
		for (Map.Entry<String, ParquetWriter<GenericRecord>> entry : writerMap.entrySet()) {
			try {
				entry.getValue().close();
			}
			catch (IOException e) {
				log.error(String.format("Cannot close writer for resource %s exception: %s", entry.getKey(), e));
			}
		}
	}
	
	public Schema getResourceSchema(String resourceType) {
		AvroConverter converter = getConverter(resourceType);
		Schema schema = converter.getSchema();
		log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
		return schema;
	}
	
	// Assumes every entry in the bundle have the same type.
	public List<GenericRecord> generateRecords(Bundle bundle) {
		List<GenericRecord> records = new ArrayList<>();
		if (bundle.getTotal() == 0) {
			return records;
		}
		String resourceType = bundle.getEntry().get(0).getResource().getResourceType().name();
		for (BundleEntryComponent entry : bundle.getEntry()) {
			Resource resource = entry.getResource();
			// TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
			records.add(convertToAvro(resource));
		}
		return records;
	}
	
	public GenericRecord convertToAvro(Resource resource) {
		org.hl7.fhir.dstu3.model.Resource r3Resource = org.hl7.fhir.convertors.VersionConvertor_30_40
		        .convertResource(resource, true);
		AvroConverter converter = getConverter(r3Resource.getResourceType().name());
		return (GenericRecord) converter.resourceToAvro(r3Resource);
	}
	
}
