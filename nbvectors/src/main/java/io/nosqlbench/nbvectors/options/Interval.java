package io.nosqlbench.nbvectors.options;

import picocli.CommandLine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

///  Allow either min_included..max_excluded or simply max_excluded format
public record Interval(long start, long end)  {
  public int count() {
    return (int) (end()-start());
  }

}
