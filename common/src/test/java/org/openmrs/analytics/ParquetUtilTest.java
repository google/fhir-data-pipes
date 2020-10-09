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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.hl7.fhir.dstu3.model.Bundle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParquetUtilTest {
	
	private String patientBundle;
	
	private String observationBundle;
	
	private ParquetUtil parquetUtil;
	
	private FhirContext fhirContext;
	
	@Before
	public void setup() throws IOException {
		patientBundle = Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
		observationBundle = Resources.toString(Resources.getResource("observation_bundle.json"), StandardCharsets.UTF_8);
		parquetUtil = new ParquetUtil();
		this.fhirContext = FhirContext.forDstu3();
	}
	
	@Test
	public void getResourceSchema_Patient() {
		Schema schema = parquetUtil.getResourceSchema("Patient", fhirContext);
		assertThat(schema.getField("id").toString(), notNullValue());
		assertThat(schema.getField("identifier").toString(), notNullValue());
		assertThat(schema.getField("name").toString(), notNullValue());
	}
	
	@Test
	public void getResourceSchema_Observation() {
		Schema schema = parquetUtil.getResourceSchema("Observation", fhirContext);
		assertThat(schema.getField("id").toString(), notNullValue());
		assertThat(schema.getField("identifier").toString(), notNullValue());
		assertThat(schema.getField("basedOn").toString(), notNullValue());
		assertThat(schema.getField("subject").toString(), notNullValue());
		assertThat(schema.getField("code").toString(), notNullValue());
	}
	
	@Test
	public void getResourceSchema_Encounter() {
		Schema schema = parquetUtil.getResourceSchema("Encounter", fhirContext);
		assertThat(schema.getField("id").toString(), notNullValue());
		assertThat(schema.getField("identifier").toString(), notNullValue());
		assertThat(schema.getField("status").toString(), notNullValue());
		assertThat(schema.getField("class").toString(), notNullValue());
	}
	
	@Test
	public void generateRecords_BundleOfPatients() {
		IParser parser = fhirContext.newJsonParser();
		Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
		List<GenericRecord> recordList = parquetUtil.generateRecords(bundle, fhirContext);
		assertThat(recordList.size(), equalTo(1));
		assertThat(recordList.get(0).get("address"), notNullValue());
		Collection<Object> addressList = (Collection<Object>) recordList.get(0).get("address");
		assertThat(addressList.size(), equalTo(1));
		Record address = (Record) addressList.iterator().next();
		assertThat((String) address.get("city"), equalTo("Waterloo"));
	}
	
	@Test
	public void generateRecords_BundleOfObservations() {
		IParser parser = fhirContext.newJsonParser();
		Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
		List<GenericRecord> recordList = parquetUtil.generateRecords(bundle, fhirContext);
		assertThat(recordList.size(), equalTo(6));
	}
}
