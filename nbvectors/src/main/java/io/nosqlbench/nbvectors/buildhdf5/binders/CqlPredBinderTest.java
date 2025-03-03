package io.nosqlbench.nbvectors.buildhdf5.binders;

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


import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;

import java.util.regex.Pattern;

public class CqlPredBinderTest implements Templatizer<String,String> {
  public static final String PREDICATE = "{PREDICATE}";
  private final String baseTemplate;
  private final String[] baseparts;

  public CqlPredBinderTest(String baseTemplate) {
    this.baseTemplate = baseTemplate;
    if (!baseTemplate.contains(PREDICATE)) {
      throw new RuntimeException("Base template must contain " + PREDICATE);
    }
    this.baseparts = baseTemplate.split(Pattern.quote(PREDICATE));
  }

  @Override
  public String andPredicate(String s, PNode<?> predicate) {

    return "";
  }
}
