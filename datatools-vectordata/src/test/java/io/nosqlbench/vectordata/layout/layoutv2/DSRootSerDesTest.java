package io.nosqlbench.vectordata.layout.layoutv2;

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


import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.utils.SHARED;
import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import io.nosqlbench.vectordata.layoutv2.DSRoot;
import io.nosqlbench.vectordata.layoutv2.DSSource;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;

import static org.assertj.core.api.Assertions.assertThat;

///  Ensure that all basic fixtures are in place for automatic serialization, deserialization, and
/// equality checks.
public class DSRootSerDesTest {

  @Test
  public void testInterval() {
    DSInterval interval = new DSInterval(0, 100);
    System.out.println(interval);
    String json = SHARED.gson.toJson(interval);
    System.out.println(json);
    Type type = new TypeToken<DSInterval>() {
    }.getType();
    DSInterval reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(interval);
  }

  @Test
  public void testWindow() {
    DSWindow window = new DSWindow();
    window.addInterval(new DSInterval(0, 100));
    System.out.println(window);
    String json = SHARED.gson.toJson(window);
    System.out.println(json);
    Type type = new TypeToken<DSWindow>() {
    }.getType();
    DSWindow reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(window);
  }

  @Test
  public void testSource() {
    DSSource source = new DSSource("test", DSWindow.ALL);
    System.out.println(source);
    String json = SHARED.gson.toJson(source);
    System.out.println(json);
    Type type = new TypeToken<DSSource>() {
    }.getType();
    DSSource reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(source);
  }

  @Test
  public void testView() {
    DSView view = new DSView("test");
    view.setSource(new DSSource("test", DSWindow.ALL));
    view.setWindow(DSWindow.ALL);
    System.out.println(view);
    String json = SHARED.gson.toJson(view);
    System.out.println(json);
    Type type = new TypeToken<DSView>() {
    }.getType();
    DSView reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(view);
  }

  @Test
  public void testProfile() {
    DSProfile profile = new DSProfile();
    profile.addView("test").setSource(new DSSource("test").setWindow(DSWindow.ALL));
    System.out.println(profile);
    String json = SHARED.gson.toJson(profile);
    System.out.println(json);
    Type type = new TypeToken<DSProfile>() {
    }.getType();
    DSProfile reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(profile);
  }

  @Test
  public void testProfileGroups() {
    DSProfileGroup datasets = new DSProfileGroup();
    datasets.addProfile("profile1").addView("view1").setSource(new DSSource("kind1", DSWindow.ALL));
    System.out.println(datasets);
    String json = SHARED.gson.toJson(datasets);
    System.out.println(json);
    Type type = new TypeToken<DSProfileGroup>() {
    }.getType();
    DSProfileGroup reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(datasets);
  }

  @Test
  public void testProfileSpecificExample() {
    String json = "{\n" +
        "  \"1M\":{# profile name\n" +
        "    \"query\":{# view name\n" +
        "      \"source\":\"bigann_query.bvecs\"# must be a map with path,window\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..1000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_1M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_1M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"2M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..2000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_2M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_2M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"5M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..5000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_5M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_5M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"10M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..10000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_10M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_10M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"20M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..20000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_20M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_20M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"50M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..50000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_50M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_50M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"100M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..100000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_100M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_100M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"200M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..200000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_200M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_200M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"500M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..500000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_500M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_500M.fvecs\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"1000M\":{\n" +
        "    \"query\":{\n" +
        "      \"source\":\"bigann_query.bvecs\"\n" +
        "    },\n" +
        "    \"base\":{\n" +
        "      \"source\":\"bigann_base.bvecs\",\n" +
        "      \"window\":\"(0..1000000000)\"\n" +
        "    },\n" +
        "    \"indices\":{\n" +
        "      \"source\":\"gnd/idx_1000M.ivecs\"\n" +
        "    },\n" +
        "    \"distances\":{\n" +
        "      \"source\":\"gnd/dis_1000M.fvecs\"\n" +
        "    }\n" +
        "  }\n" +
        "}";
    DSProfileGroup profileGroup = DSProfileGroup.fromData(json);

  }

  @Test
  public void testDSRoot() {
    DSRoot root = new DSRoot("test", new DSProfileGroup());
    root.addProfile("test").addView("test").setSource(new DSSource("test", DSWindow.ALL));
    System.out.println(root);
    String json = SHARED.gson.toJson(root);
    System.out.println(json);
    Type type = new TypeToken<DSRoot>() {
    }.getType();
    DSRoot reified = SHARED.gson.fromJson(json, type);
    System.out.println(reified);
    assertThat(reified).isEqualTo(root);
  }

  @Test
  public void testProfileWithMaxkAsNumber() {
    // Test that maxk as a number (e.g., 10.0) is properly handled and doesn't cause "invalid view format" error
    String json = """
        {
          "maxk": 10.0,
          "base": {
            "source": "test.bin"
          }
        }
        """;
    DSProfile profile = DSProfile.fromData(SHARED.mapFromJson(json));
    assertThat(profile.getMaxk()).isEqualTo(10);
    assertThat(profile).containsKey("base");
    assertThat(profile.get("base").getSource().path).isEqualTo("test.bin");
  }

  @Test
  public void testProfileWithMaxkAsString() {
    // Test that maxk as a string is properly parsed
    String json = """
        {
          "maxk": "20",
          "base": {
            "source": "test.bin"
          }
        }
        """;
    DSProfile profile = DSProfile.fromData(SHARED.mapFromJson(json));
    assertThat(profile.getMaxk()).isEqualTo(20);
    assertThat(profile).containsKey("base");
  }

  @Test
  public void testProfileWithoutMaxk() {
    // Test that profiles without maxk still work
    String json = """
        {
          "base": {
            "source": "test.bin"
          }
        }
        """;
    DSProfile profile = DSProfile.fromData(SHARED.mapFromJson(json));
    assertThat(profile.getMaxk()).isNull();
    assertThat(profile).containsKey("base");
  }

  @Test
  public void testProfileGroupWithMaxk() {
    // Test the full stack with a profile group containing maxk
    String json = """
        {
          "default": {
            "maxk": 10,
            "base": {
              "source": "base.bin"
            },
            "indices": {
              "source": "indices.bin"
            }
          }
        }
        """;
    DSProfileGroup profileGroup = DSProfileGroup.fromData(json);
    assertThat(profileGroup).containsKey("default");
    DSProfile defaultProfile = profileGroup.get("default");
    assertThat(defaultProfile.getMaxk()).isEqualTo(10);
    assertThat(defaultProfile).containsKey("base");
    assertThat(defaultProfile).containsKey("indices");
    assertThat(defaultProfile).doesNotContainKey("maxk"); // maxk should not be treated as a view
  }
}
