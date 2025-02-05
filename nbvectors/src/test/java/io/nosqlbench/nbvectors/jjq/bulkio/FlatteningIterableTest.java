package io.nosqlbench.nbvectors.jjq.bulkio;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;

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