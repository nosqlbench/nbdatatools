package io.nosqlbench.command.jjq.bulkio;

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


import io.nosqlbench.nbdatatools.api.iteration.FlatteningIterable;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class FlatteningIterableTest {

  @Test
  public void testFlatteningNativeIterable() {
    List<List<String>> lls = new ArrayList<>();
    lls.add(List.of("a", "b", "c"));
    lls.add(List.of("d", "e", "f"));
    // reifying the iterable inner type is automatic if you don't pass a function
    // Function<List<String>,Iterable<String>> f = lolos -> lolos;
    FlatteningIterable<List<String>, String> fi = new FlatteningIterable<>(lls);
    List<String> flattened = new ArrayList<>();
    fi.forEach(flattened::add);
    assertThat(flattened).containsExactly("a", "b", "c", "d", "e", "f");
  }

  @Test
  public void testFlatteningOpaqueIterable() {
    List<String> blocks = List.of(
        """
            a
            b
            c
            """, """
            d
            e
            f
            """
    );

    FlatteningIterable<String, String> fi =
        new FlatteningIterable<>(blocks, (String s) -> Arrays.asList(s.split("\n")));
    List<String> flattened = new ArrayList<>();
    fi.forEach(flattened::add);
    assertThat(flattened).containsExactly("a", "b", "c", "d", "e", "f");


  }

}
