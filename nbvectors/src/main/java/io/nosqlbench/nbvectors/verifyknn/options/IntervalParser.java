package io.nosqlbench.nbvectors.verifyknn.options;

import picocli.CommandLine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IntervalParser implements CommandLine.ITypeConverter<Interval> {
  final static Pattern format = Pattern.compile("^((?<start>\\d+)\\.\\.)?(?<end>\\d+)$");

  public IntervalParser() {
  }

  @Override
  public Interval convert(String value) throws Exception {
    Matcher matcher = format.matcher(value);
    if (matcher.matches()) {
      String start = matcher.group("start");
      start = (start != null) ? start : "0";
      String end = matcher.group("end");
      return new Interval(Long.parseLong(start), Long.parseLong(end));
    } else {
      throw new IllegalArgumentException("Invalid interval: " + value);
    }
  }
}
