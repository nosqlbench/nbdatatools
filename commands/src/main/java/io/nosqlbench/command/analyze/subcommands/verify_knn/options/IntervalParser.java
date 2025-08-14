package io.nosqlbench.command.analyze.subcommands.verify_knn.options;

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


import picocli.CommandLine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// A parser for the {@link Interval} type
public class IntervalParser implements CommandLine.ITypeConverter<Interval> {
  final static Pattern format = Pattern.compile("^((?<minIncl>\\d+)\\.\\.)?(?<maxExcl>\\d+)$");

  /// create an intervals parser
  public IntervalParser() {
  }

  /// convert a string to an intervals, using the format {@code minIncl..maxExcl}, or {@code maxExcl}
  /// For example, `5` means `0..5`, to include 0, 1, 2, 3, and 4.
  @Override
  public Interval convert(String value) {
    Matcher matcher = format.matcher(value);
    if (matcher.matches()) {
      String minIncl = matcher.group("minIncl");
      minIncl = (minIncl != null) ? minIncl : "0";
      String maxExcl = matcher.group("maxExcl");
      return new Interval(Long.parseLong(minIncl), Long.parseLong(maxExcl));
    } else {
      throw new IllegalArgumentException("Invalid intervals: " + value);
    }
  }
}
