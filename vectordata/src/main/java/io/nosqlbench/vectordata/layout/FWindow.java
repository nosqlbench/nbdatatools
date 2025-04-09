package io.nosqlbench.vectordata.layout;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public record FWindow(List<FInterval> intervals) {

  public final static Pattern PATTERN;
  public static final FWindow ALL = new FWindow(List.of(new FInterval(-1, -1)));

  static {
    String regex = """
        ^
        [(\\[]? \\s*
        (?<intervals>(.)+)
        [)\\]]? \\s*
        $
        """;
    PATTERN = Pattern.compile(regex, Pattern.COMMENTS | Pattern.DOTALL);
  }

  public FWindow(String... specs) {
    this(parseSpecs(specs));
  }

  private static List<FInterval> parseSpecs(String[] specs) {
    List<FInterval> intervals = new ArrayList<>(specs.length);
    for (String spec : specs) {
      intervals.add(FInterval.parse(spec));
    }
    return intervals;
  }

  public static FWindow parse(String spec) {
    if (spec == null) {
      return FWindow.ALL;
    }
    Matcher matcher = PATTERN.matcher(spec);
    if (matcher.matches()) {
      List<FInterval> interval = new ArrayList<>();
      String intervalSpecs = matcher.group("intervals");
      String[] specs = intervalSpecs.split(" *, *");
      for (String s : specs) {
        interval.add(FInterval.parse(s));
      }
      return new FWindow(interval);
    } else {
      throw new RuntimeException("""
          "Invalid data window spec: 'SPEC'. Expected a comma separated list of intervals.
          Each interval is of the form 'START..END', where START and END are integers.
          The start value in inclusive and the end value is exclusive. The whole window spec can be wrapped in
          square or round brackets.
          """.replaceAll("SPEC", spec));
    }
  }

  public static FWindow fromObject(Object window) {
    if (window instanceof FWindow fw) {
      return fw;
    } else if (window == null) {
      return FWindow.ALL;
    } else if (window instanceof CharSequence cs) {
      return parse(cs.toString());
    } else if (window instanceof Number n) {
      return new FWindow(List.of(new FInterval(0, n.longValue())));
    } else if (window instanceof List<?> l) {
      return new FWindow(l.stream().map(FInterval::fromObject).toList());
    } else if (window instanceof Map<?, ?> m) {
      return fromObject(m.get("intervals"));
    } else {
      throw new RuntimeException("invalid window format:" + window);
    }
  }

  public String toData() {
    return this.intervals().stream().map(FInterval::toData).collect(Collectors.joining(","));
  }

  public long translate(long index) {
    return intervals.get(0).minIncl() + index;
  }
}
