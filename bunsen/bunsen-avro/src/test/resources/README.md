This directory contains the structure definitions and schemas required for the
test cases in the module `bunsen-avro`.

In particular, the avro schemas in this directory are generated using the class
`com.cerner.bunsen.avro.tools.GenerateAggregatedSchemas`. For example, to
generate the avro schemas for the resource types `Patient` and `Observation`
belonging to the `US Core profile` and `R4 fhir Version`, run the class
`com.cerner.bunsen.avro.tools.GenerateAggregatedSchemas` with the below
arguments
`fhirVersion=R4 structureDefinitionsPath=/r4-us-core-definitions isClasspath=true resourceTypes=Patient,Observation outputDir=/Users/sankarapu/sourcecode/temp`
