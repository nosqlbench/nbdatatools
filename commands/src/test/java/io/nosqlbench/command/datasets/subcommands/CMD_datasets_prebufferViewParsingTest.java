package io.nosqlbench.command.datasets.subcommands;

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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/// Tests for view parsing in {@link CMD_datasets_prebuffer} to ensure synonyms map correctly.
public class CMD_datasets_prebufferViewParsingTest {

  private CMD_datasets_prebuffer command;
  private Field viewsField;
  private Method parseViewsMethod;

  @BeforeEach
  public void setUp() throws Exception {
    command = new CMD_datasets_prebuffer();
    viewsField = CMD_datasets_prebuffer.class.getDeclaredField("views");
    viewsField.setAccessible(true);
    parseViewsMethod = CMD_datasets_prebuffer.class.getDeclaredMethod("parseViews");
    parseViewsMethod.setAccessible(true);
  }

  @SuppressWarnings("unchecked")
  private List<String> invokeParseViews(String value) throws Exception {
    viewsField.set(command, value);
    return (List<String>) parseViewsMethod.invoke(command);
  }

  @Test
  public void shouldReturnWildcardWhenUnset() throws Exception {
    assertEquals(List.of("*"), invokeParseViews("*"));
    assertEquals(List.of("*"), invokeParseViews("   "));
  }

  @Test
  public void shouldNormalizeViewSynonyms() throws Exception {
    List<String> views = invokeParseViews("base,query,indices,distances");
    assertIterableEquals(
        List.of("base_vectors", "query_vectors", "neighbor_indices", "neighbor_distances"),
        views
    );
  }

  @Test
  public void shouldHandleMixedCaseAndWhitespace() throws Exception {
    List<String> views = invokeParseViews("  Base_Vectors , Query , Neighbor_Indices , Neighbors  ");
    assertIterableEquals(
        List.of("base_vectors", "query_vectors", "neighbor_indices", "neighbor_indices"),
        views
    );
  }
}
