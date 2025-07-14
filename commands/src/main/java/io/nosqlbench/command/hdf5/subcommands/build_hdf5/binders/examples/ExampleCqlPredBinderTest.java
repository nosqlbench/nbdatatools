package io.nosqlbench.command.hdf5.subcommands.build_hdf5.binders.examples;

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


import io.nosqlbench.command.hdf5.subcommands.build_hdf5.binders.PredicateTemplatizer;
import io.nosqlbench.vectordata.spec.predicates.PNode;


import java.util.regex.Pattern;

/// test cql predicate binding - experimental
public class ExampleCqlPredBinderTest implements PredicateTemplatizer<String,String> {

  /// The string to replace in the template with the predicate syntax
  public static final String PREDICATE = "{PREDICATE}";
  /// A base template which contains {@link #PREDICATE}
  private final String baseTemplate;
  /// The template literals in between and around {@link #PREDICATE}
  private final String[] baseparts;

  /// Experimental type for binding
  /// @param baseTemplate a base CQL template string, to be extended with predicate syntax
  public ExampleCqlPredBinderTest(String baseTemplate) {
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
