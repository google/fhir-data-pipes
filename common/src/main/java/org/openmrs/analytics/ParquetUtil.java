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
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.avro.AvroConverter;
import com.google.common.annotations.VisibleForTesting;
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
	
	private static final Pattern OUTPUT_PATTERN = Pattern.compile(".*output-streaming-([\\p{Digit}]{5}+)");
	
	private final FhirContext fhirContext;
	
	private final Map<String, AvroConverter> converterMap;
	
	private final Map<String, ParquetWriter<GenericRecord>> writerMap;
	
	private final int rowGroupSize;
	
	private final String parquetFilePath;
	
	private final FileSystem fileSystem;
	
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
	
	/**
	 * Note using this constructor may cause the files not be be flushed until `closeAllWriters` is
	 * called.
	 *
	 * @param parquetFilePath The directory under which the Parquet files are written.
	 */
	public ParquetUtil(String parquetFilePath) {
		this(parquetFilePath, 0, 0, FileSystems.getDefault());
	}
	
	/**
	 * The preferred constructor for the streaming mode.
	 *
	 * @param parquetFilePath The directory under which the Parquet files are written.
	 * @param secondsToFlush The interval after which the content of Parquet writers is flushed to disk.
	 * @param rowGroupSize The approximate size of row-groups in the Parquet files (0 means use
	 *            default).
	 */
	public ParquetUtil(String parquetFilePath, int secondsToFlush, int rowGroupSize) {
		this(parquetFilePath, secondsToFlush, rowGroupSize, FileSystems.getDefault());
	}
	
	@VisibleForTesting
	ParquetUtil(String parquetFilePath, int secondsToFlush, int rowGroupSize, FileSystem fileSystem) {
		this.fhirContext = FhirContext.forDstu3();
		this.converterMap = new HashMap<>();
		this.writerMap = new HashMap<>();
		this.parquetFilePath = parquetFilePath;
		this.rowGroupSize = rowGroupSize;
		this.fileSystem = fileSystem;
		if (secondsToFlush > 0) {
			TimerTask task = new TimerTask() {
				
				@Override
				public void run() {
					try {
						flushAll();
					}
					catch (IOException e) {
						log.error("Could not flush Parquet files: " + e);
					}
				}
			};
			new Timer().scheduleAtFixedRate(task, secondsToFlush * 1000, secondsToFlush * 1000);
		}
	}
	
	synchronized private AvroConverter getConverter(String resourceType) {
		if (!converterMap.containsKey(resourceType)) {
			AvroConverter converter = AvroConverter.forResource(fhirContext, resourceType);
			converterMap.put(resourceType, converter);
		}
		return converterMap.get(resourceType);
	}
	
	@VisibleForTesting
	synchronized Path bestOutputFile(String resourceType) throws IOException {
		java.nio.file.Path outputDir = fileSystem.getPath(getParquetPath(), resourceType);
		Files.createDirectories(outputDir);
		Stream<java.nio.file.Path> files = Files.list(outputDir);
		Optional<Integer> maxIndex = files.map(f -> {
			Matcher matcher = OUTPUT_PATTERN.matcher(f.toString());
			if (matcher.matches()) {
				return new Integer(matcher.group(1).replaceFirst("^0+(?!$)", ""));
			}
			return -1;
		}).max(Integer::compareTo);
		int bestIndex = maxIndex.map(integer -> integer + 1).orElse(0);
		if (bestIndex > 99999) {
			// TODO: Handle cases with more than 100K files in a directory, e.g., by creating an extra
			// layer in the directory hierarchy.
			throw new IOException(
			        "It is not wise to create more than 100K files in a directory, please adjust your configuration params!");
		}
		String bestFileName = String.format("output-streaming-%05d", bestIndex);
		Path bestFilePath = new Path(Paths.get(getParquetPath(), resourceType).toString(), bestFileName);
		log.info("Creating new Parguet file " + bestFilePath);
		return bestFilePath;
	}
	
	synchronized private void craeteWriter(String resourceType) throws IOException {
		// TODO: Find a way to convince Hadoop file operations to use `fileSystem` (needed for testing).
		AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(bestOutputFile(resourceType));
		// TODO: Adjust other parquet file parameters for our needs or make them configurable.
		if (rowGroupSize > 0) {
			builder.withRowGroupSize(rowGroupSize);
		}
		ParquetWriter<GenericRecord> writer = builder.withSchema(getResourceSchema(resourceType)).build();
		writerMap.put(resourceType, writer);
	}
	
	/**
	 * This is to write a FHIR resource to a Parquet file. This automatically handles file creation in a
	 * directory named after the resource type (e.g., `Patient`) with names following the
	 * "output-streaming-ddddd" pattern where `ddddd` is a string with five digits. NOTE: This should
	 * not be used in its current form once we move the streaming pipeline to Beam; the I/O should be
	 * left to Beam similar to the batch mode.
	 */
	synchronized public void write(Resource resource) throws IOException {
		Preconditions.checkNotNull(getParquetPath());
		Preconditions.checkNotNull(resource.fhirType());
		String resourceType = resource.fhirType();
		if (!writerMap.containsKey(resourceType)) {
			craeteWriter(resourceType);
		}
		final ParquetWriter<GenericRecord> parquetWriter = writerMap.get(resourceType);
		parquetWriter.write(this.convertToAvro(resource));
	}
	
	synchronized private void flush(String resourceType) throws IOException {
		ParquetWriter<GenericRecord> writer = writerMap.get(resourceType);
		if (writer != null && writer.getDataSize() > 0) {
			writer.close();
			craeteWriter(resourceType);
		}
	}
	
	synchronized private void flushAll() throws IOException {
		for (String resourceType : writerMap.keySet()) {
			flush(resourceType);
		}
	}
	
	synchronized public void closeAllWriters() throws IOException {
		for (Map.Entry<String, ParquetWriter<GenericRecord>> entry : writerMap.entrySet()) {
			entry.getValue().close();
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
	
	@VisibleForTesting
	GenericRecord convertToAvro(Resource resource) {
		org.hl7.fhir.dstu3.model.Resource r3Resource = org.hl7.fhir.convertors.VersionConvertor_30_40
		        .convertResource(resource, true);
		AvroConverter converter = getConverter(r3Resource.getResourceType().name());
		return (GenericRecord) converter.resourceToAvro(r3Resource);
	}
	
}
