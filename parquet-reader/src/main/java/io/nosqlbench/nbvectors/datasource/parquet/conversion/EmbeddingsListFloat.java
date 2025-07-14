package io.nosqlbench.nbvectors.datasource.parquet.conversion;

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


import org.apache.parquet.example.data.Group;

import java.util.function.Function;

/// This is a converter that will read the following format to extract a float vector:
/// ```parquet schema
///   ...
///   optional group embeddings (LIST) {
///     repeated group list {
///       optional float element;
///     }
///   }
///   ...
/// ```
public class EmbeddingsListFloat implements Function<Group, float[]> {
  @Override
  public float[] apply(Group group) {
    Group embeddings = group.getGroup("embeddings", 0);
    int repetition = embeddings.getFieldRepetitionCount("list");
    float[] floats = new float[repetition];

    for (int i = 0; i < repetition; i++) {
      Group listGroup = embeddings.getGroup("list", i);
      float element = listGroup.getFloat("element", 0);
      floats[i] = element;
    }
    return floats;
  }
}
