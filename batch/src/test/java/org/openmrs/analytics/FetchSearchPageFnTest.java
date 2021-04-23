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
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.TupleTag;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FetchSearchPageFnTest {
	
	private FetchSearchPageFn<String> fetchSearchPageFn;
	
	private FhirContext fhirContext;
	
	@Mock
	private MultiOutputReceiver multiOutputReceiver;
	
	@Mock
	private OutputReceiver<GenericRecord> outputReceiver;
	
	@Captor
	private ArgumentCaptor<GenericRecord> recordCaptor;
	
	@Before
	public void setUp() {
		String[] args = { "--outputParquetPath=SOME_PATH" };
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		fetchSearchPageFn = new FetchSearchPageFn<String>(options, "TEST") {};
		this.fhirContext = FhirContext.forR4();
		when(multiOutputReceiver.get(any(TupleTag.class))).thenReturn(outputReceiver);
		fetchSearchPageFn.setup();
	}
	
	@Test
	public void testProcessObservationBundle() throws IOException {
		String observationBundleStr = Resources.toString(Resources.getResource("observation_decimal_bundle.json"),
		    StandardCharsets.UTF_8);
		IParser parser = fhirContext.newJsonParser();
		Bundle bundle = parser.parseResource(Bundle.class, observationBundleStr);
		fetchSearchPageFn.processBundle(bundle, multiOutputReceiver);
		
		// Verify the generated record has the right "value".
		verify(outputReceiver).output(recordCaptor.capture());
		GenericRecord record = recordCaptor.getValue();
		GenericData.Record valueRecord = (GenericData.Record) record.get("value");
		BigDecimal value = (BigDecimal) ((GenericData.Record) valueRecord.get("quantity")).get("value");
		assertThat(value.doubleValue(), closeTo(1.8287, 0.001));
	}
	
}
