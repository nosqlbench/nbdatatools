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


import io.nosqlbench.vectordata.api.UnitConversions;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record FInterval(long minIncl, long maxExcl) {
  public final static Pattern PATTERN = Pattern.compile(
      """
          [(\\[]? \\s*
          (?<start>\\d[\\d_]*\\w*) \\s*
          ((\\.\\.|-|→) \\s*
          (?<end>\\d[\\d_]*\\w*))? \\s*
          [)\\]]? \\s*
          """, Pattern.COMMENTS | Pattern.DOTALL
  );

  public static FInterval parse(String interval) {
    Matcher matcher = PATTERN.matcher(interval);
    if (matcher.matches()) {
      String u1 = matcher.group("start").replaceAll("_", "");
      long start = UnitConversions.longCountFor(u1)
          .orElseThrow(() -> new RuntimeException("invalid intervals format:" + interval));
      if (matcher.group("end") == null) {
        return new FInterval(0, start);
      }

      String u2 = matcher.group("end").replaceAll("_", "");
      long end = UnitConversions.longCountFor(u2)
          .orElseThrow(() -> new RuntimeException("invalid " + "intervals format:" + interval));
      return new FInterval(start, end);
    }
    throw new RuntimeException("invalid intervals format:" + interval + ", expected [start..end] "
                               + "or any similar pattern with optional ( or [, digits, .. or - or "
                               + "→, digits, and optional ) or ], like '[10..1000)', or '10 → 20' "
                               + "for example.");

  }

  public String toData() {
    if (minIncl == -1L && maxExcl == -1L) {
      return "";
    }
    return "(" + minIncl + ".." + maxExcl + ")";
  }

  public int count() {
    return (int) (maxExcl - minIncl);
  }

  public static FInterval fromObject(Object o) {
    if (o instanceof String s) {
      return parse(s);
    } else if (o instanceof Number n) {
      return new FInterval(0, n.longValue());
    } else if (o instanceof FInterval fi) {
      return fi;
    } else if (o instanceof Map<?, ?> map) {
      Object min = map.get("minIncl");
      Object max = map.get("maxExcl");
      if (min instanceof Number minn && max instanceof Number maxn) {
        return new FInterval(minn.longValue(), maxn.longValue());
      } else if (min instanceof String minstr && max instanceof String maxstr) {
        return new FInterval(Long.parseLong(minstr), Long.parseLong(maxstr));
      } else {
        throw new RuntimeException("invalid intervals format:" + o);
      }
    } else {
      throw new RuntimeException("invalid intervals format:" + o);
    }
  }
}
