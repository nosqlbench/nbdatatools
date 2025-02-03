package io.nosqlbench.nbvectors.taghdf.attrtypes;

import picocli.CommandLine;

public class AttrSetConverter implements CommandLine.ITypeConverter<AttrSet> {
  @Override
  public AttrSet convert(String value) throws Exception {
    return AttrSet.parse(value);
  }
}
