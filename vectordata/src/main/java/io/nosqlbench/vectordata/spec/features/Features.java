package io.nosqlbench.vectordata.spec.features;

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


import java.util.Set;

/// Features are well-known attributes of a dataset
public enum Features {
  /// The associated data contains some vectors where all components are set to 0.0
  CONTAINS_ZEROES,
  /// The associated data contains some vectors which are duplicates
  CONTAINS_DUPLICATES,
  /// The associated data contains vectors that have been normalized
  NORMALIZED,
  /// The associated data has been shuffled from the original ordering
  SHUFFLED;

  public static long getFeatureMask(Set<Features> features) {
    long mask = 0L;
    for (Features feature : features) {
      mask |= 1L << feature.ordinal();
    }
    return mask;
  }

}

