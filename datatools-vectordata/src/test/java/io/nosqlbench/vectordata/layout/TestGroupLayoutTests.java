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


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGroupLayoutTests {

  private final static FSource fs1 = new FSource("test1.fvec", "[0..1_000_000,0..128)");
  private final static FSource fs2 = new FSource("test1.fvec", "[0..1_000_000,0..128)");
  private final static FWindow fw1 = new FWindow("5..100", "0..90");
  private final static FWindow fw2 = new FWindow("10..100", "0..80");
  private final static FView fv1 = new FView(fs1, fw1);
  private final static FView fv2 = new FView(fs2, fw2);
  private final static FProfiles fp1 =
      new FProfiles(Map.of("base_vectors", fv1, "query_vectors", fv2));
  private final static FProfiles fp2 =
      new FProfiles(Map.of("base_vectors", fv1, "query_vectors", fv2));
  private final static FGroup fc1 = new FGroup(Map.of("p1", fp1, "p2", fp2));


  @Test
  public void testJsonFormat() {
    String json = fc1.toJson();
    System.out.println(json);
  }

  @Test
  public void testYamlFormat() {
    String yaml = "attributes:\n" +
        "  model: testmodel\n" +
        "  url: testurl\n" +
        "  distance_function: COSINE\n" +
        "  license: testlicense\n" +
        "  vendor: testvendor\n" +
        "  notes: testnotes\n" +
        "profiles:\n" +
        "  p1:\n" +
        "    base_vectors:\n" +
        "      source: test1.fvec(0..1_000_000,0..128)\n" +
        "      window: (5..100,0..90)\n" +
        "    query_vectors:\n" +
        "      source: test1.fvec(0..1_000_000,0..128)\n" +
        "      window: (5..100,0..90)\n" +
        "  p2:\n" +
        "    base_vectors:\n" +
        "      source: test1.fvec(0..1_000_000,0..128)\n" +
        "      window: (10..100,0..80)";
    TestGroupLayout fgc = TestGroupLayout.fromYaml(yaml);
    //    assertThat(fc2).isEqualTo(fc1);

  }

  @Test
  public void testFSource() {
    FSource fs1 = new FSource("bigann_base.bvecs", "[0..1_000_000,0..128)");
    FSource fs2 = FSource.parse("bigann_base.bvecs[0..1_000_000,0..128)");
    assertThat(fs1).isEqualTo(fs2);
  }

}
