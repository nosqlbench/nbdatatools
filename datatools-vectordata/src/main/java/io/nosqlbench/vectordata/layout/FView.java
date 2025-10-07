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


import java.util.LinkedHashMap;
import java.util.Map;

/// An FView is a view of a source, with an optional window.
/// @see FSource
public class FView {
  /// the source of the view
  private final FSource source;
  /// the window of the view
  private final FWindow window;

  public FView(FSource source, FWindow window) {
    this.source = source;
    this.window = window;
  }

  public FSource source() {
    return source;
  }

  public FWindow window() {
    return window;
  }

  /// loads a view from object data
  /// @param pv the object data to load from
  /// @return a view
  public static FView fromObject(Object pv) {
    if (pv instanceof CharSequence) {
      CharSequence cs = (CharSequence) pv;
      return new FView(new FSource(cs.toString(), FWindow.ALL), FWindow.ALL);
    } else if (pv instanceof Map<?, ?>) {
      Map<?, ?> m = (Map<?, ?>) pv;
      FSource source = FSource.fromObject(m.get("source"));
      FWindow window = FWindow.fromObject(m.get("window"));
      return new FView(source, window);
    } else {
      throw new RuntimeException("invalid view format:" + pv);
    }
  }

  /// provide raw data for this view
  /// @return the raw data for this view
  public Map<String, Object> toData() {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("source", source.toData());
    if (window != FWindow.ALL) {
      map.put("window", window.toData());
    }
    return map;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FView that = (FView) obj;
    return java.util.Objects.equals(source, that.source) && java.util.Objects.equals(window, that.window);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(source, window);
  }

  @Override
  public String toString() {
    return "FView{source=" + source + ", window=" + window + '}';
  }
}
