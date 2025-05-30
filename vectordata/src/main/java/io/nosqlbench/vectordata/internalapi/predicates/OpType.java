package io.nosqlbench.vectordata.internalapi.predicates;

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

/// The operator type
public enum OpType {
  /// greater than `>`
  GT(">"),
  /// less than `<`
  LT("<"),
  /// equals `=`
  EQ("="),
  /// not equals `!=`
  NE("!="),
  /// greater than or equal `>=`
  GE(">="),
  /// less than or equal `<=`
  LE("<="),
  /// in set
  IN("IN");

  private final String symbol;

  OpType(String symbol) {
    this.symbol=symbol;
  }

  /// get the symbol for this operator
  /// @return the symbol for this operator
  /// @see #symbol
  public String symbol() {
    return this.symbol;
  }
}
