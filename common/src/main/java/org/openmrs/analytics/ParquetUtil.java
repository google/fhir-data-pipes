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

import java.util.ArrayList;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.avro.AvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.dstu3.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetUtil {
	
	private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);
	
	public Schema getResourceSchema(String resourceType, FhirContext fhirContext) {
		AvroConverter converter = AvroConverter.forResource(fhirContext, resourceType);
		Schema schema = converter.getSchema();
		log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
		return schema;
	}
	
	// Assumes every entry in the bundle have the same type.
	public List<GenericRecord> generateRecords(Bundle bundle, FhirContext fhirContext) {
		List<GenericRecord> records = new ArrayList<>();
		if (bundle.getTotal() == 0) {
			return records;
		}
		String resourceType = bundle.getEntry().get(0).getResource().getResourceType().name();
		AvroConverter converter = AvroConverter.forResource(fhirContext, resourceType);
		for (BundleEntryComponent entry : bundle.getEntry()) {
			Resource resource = entry.getResource();
			// TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
			records.add((GenericRecord) converter.resourceToAvro(resource));
		}
		return records;
	}
	
}
