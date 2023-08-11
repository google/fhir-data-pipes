package com.cerner.bunsen.definitions;

import java.util.List;

/**
 * The minimal interface from StructureDefinition that is needed for type traversal. The expected
 * pattern is to encapsulate a FHIR version specific StructureDefinition by an IStructureDefinition.
 */
public interface IStructureDefinition {

  String getUrl();

  String getType();

  IElementDefinition getRootDefinition();

  List<IElementDefinition> getSnapshotDefinitions();
}
