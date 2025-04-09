package io.nosqlbench.vectordata;

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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.layout.FInterval;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import org.snakeyaml.engine.v2.constructor.StandardConstructor;
import org.snakeyaml.engine.v2.nodes.Tag;
import org.snakeyaml.engine.v2.representer.StandardRepresenter;

import java.lang.reflect.Type;
import java.util.Map;

public class SHARED {
  public final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final static LoadSettings loadSettings = LoadSettings.builder().setLabel("load").build();
  public final static Load yamlLoader = new Load(loadSettings);
  private final static DumpSettings dumpSettings =
      DumpSettings.builder().setDefaultFlowStyle(FlowStyle.BLOCK).build();
  private final static StandardRepresenter representer = new StandardRepresenter(dumpSettings) {{
    addClassTag(FInterval.class, new Tag("!finterval"));
  }};
  private final static StandardConstructor constructor = new StandardConstructor(loadSettings);
  public final static Dump yamlDumper = new Dump(dumpSettings, representer);

  public static Map<String,?> mapFromJson(String profilesData) {
    Type type = new TypeToken<Map<String,?>>(){}.getType();
    Map<String,?> map = gson.fromJson(profilesData, type);
    return map;
  }
}
