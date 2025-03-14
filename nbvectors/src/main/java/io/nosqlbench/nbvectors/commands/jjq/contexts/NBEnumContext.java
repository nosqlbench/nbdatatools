package io.nosqlbench.nbvectors.commands.jjq.contexts;

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


import java.util.concurrent.atomic.AtomicLong;

/// A stateful context for enumerating values
public class NBEnumContext implements NBIdEnumerator {
  private final String fieldName;

  /// create a new enumeration context
  /// @param fieldName the name of the field to enumerate
  public NBEnumContext(String fieldName) {
    this.fieldName = fieldName;
  }
  private final AtomicLong value = new AtomicLong(0L);
  @Override
  public long getAsLong() {
    return value.getAndIncrement();
  }

  @Override
  public String toString() {
    return "Enum(" + this.fieldName + "):@" + value.get();
  }
}
