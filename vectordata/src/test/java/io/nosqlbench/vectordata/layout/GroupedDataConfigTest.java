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


import org.assertj.core.util.URLs;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupedDataConfigTest {

  @Test
  public void testFullConfig() {
    URL resource = getClass().getClassLoader().getResource("groupconfig.yaml");
    String contents = URLs.contentOf(resource, StandardCharsets.UTF_8);
    TestGroupLayout fgc = TestGroupLayout.fromYaml(contents);
    assertThat(fgc).isNotNull();
    FGroup fGroupConfig = fgc.profiles();
    assertThat(fGroupConfig).isNotNull();
    Map<String, FProfiles> profiles = fGroupConfig.profiles();
    assertThat(profiles.keySet()).containsExactly(
        "1M",
        "2M",
        "5M",
        "10M",
        "20M",
        "50M",
        "100M",
        "200M",
        "500M",
        "1000M"
    );
    Map<String, Long> sizes = new LinkedHashMap<>() {{
      put("1M", 1_000_000L);
      put("2M", 2_000_000L);
      put("5M", 5_000_000L);
      put("10M", 10_000_000L);
      put("20M", 20_000_000L);
      put("50M", 50_000_000L);
      put("100M", 100_000_000L);
      put("200M", 200_000_000L);
      put("500M", 500_000_000L);
      put("1000M", 1_000_000_000L);
    }};
    sizes.forEach((k, v) -> {
      assertThat(profiles.get(k).views().get("base").source()
          .window()).isEqualTo(FWindow.ALL);
      assertThat(profiles.get(k).views().get("base").window()).isEqualTo(new FWindow(List.of(new FInterval(0L, v))));

    });


    for (FProfiles value : profiles.values()) {
      assertThat(value.views().keySet()).containsExactly("base", "indices", "distances");
      assertThat(value.views().get("base").source()).isEqualTo(new FSource(
          "bigann_base.bvecs",
          FWindow.ALL
      ));
    }
  }

  @Test
  public void testYamlDump() {
    URL resource = getClass().getClassLoader().getResource("groupconfig.yaml");
    String contents = URLs.contentOf(resource, StandardCharsets.UTF_8);
    TestGroupLayout fgc = TestGroupLayout.fromYaml(contents);
    String yaml = fgc.toYaml();
    System.out.println(yaml);
    TestGroupLayout reified = TestGroupLayout.fromYaml(yaml);
    assertThat(reified).isEqualTo(fgc);
  }

}
