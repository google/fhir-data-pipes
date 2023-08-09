package com.cerner.bunsen.definitions;

import java.util.List;

public interface IStructureDefinition {

  String getUrl();

  String getType();

  IElementDefinition getRootDefinition();

  List<IElementDefinition> getSnapshotDefinitions();

}
