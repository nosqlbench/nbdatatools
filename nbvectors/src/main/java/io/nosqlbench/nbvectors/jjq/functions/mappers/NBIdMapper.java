package io.nosqlbench.nbvectors.jjq.functions.mappers;

public interface NBIdMapper {

  void addInstance(String fieldName, String string);
  void finish();
  long lookupId(String fieldName, String text);

}
