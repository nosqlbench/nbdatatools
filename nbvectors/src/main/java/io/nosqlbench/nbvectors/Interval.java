package io.nosqlbench.nbvectors;

import picocli.CommandLine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

///  Allow either min_included..max_excluded or simply max_excluded format
public record Interval(long start, long end)  {
  public static class Converter implements CommandLine.ITypeConverter<Interval> {
    final static Pattern format = Pattern.compile("^((?<start>\\d+)\\.\\.)?(?<end>\\d+)$");
    public Converter() {}
    @Override
    public Interval convert(String value) throws Exception {
      Matcher matcher = format.matcher(value);
      if (matcher.matches()) {
        String start = matcher.group("start");
        start = (start!=null) ? start : "0";
        String end = matcher.group("end");
        return new Interval(Long.parseLong(start), Long.parseLong(end));
      } else {
        throw new IllegalArgumentException("Invalid interval: " + value);
      }
    }
  }
}
