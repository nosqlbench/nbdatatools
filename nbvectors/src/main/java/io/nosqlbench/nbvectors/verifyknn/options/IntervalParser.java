package io.nosqlbench.nbvectors.verifyknn.options;

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
