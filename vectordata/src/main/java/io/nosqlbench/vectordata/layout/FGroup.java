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


import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.SHARED;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

public record FGroup(Map<String, FProfiles> profiles) {
  private final static Type type = new TypeToken<>(){}.getType();
  public static FGroup fromJSON(String json) {

    Map<String, FProfiles> facets = SHARED.gson.fromJson(json, type);
    return new FGroup(facets);
  }

  public static FGroup fromObject(Object facetsObject, FProfiles defaultProfile) {
    if (facetsObject instanceof Map<?,?> m) {
      Map<String, FProfiles> facets = new LinkedHashMap<>();
      m.forEach((k,v) -> {
        String profileName = k.toString();
        try {
          FProfiles fprofile = FProfiles.fromObject(v,defaultProfile);
          facets.put(profileName,fprofile);
        } catch (Exception e) {
          throw new RuntimeException("invalid profile format for key[" + k + "]:" + v, e);
        }
      });
      return new FGroup(facets);
    }
    return null;
  }

  public String toJson() {
    return SHARED.gson.toJson(this);
  }

  public Object toData() {
    Map<String,Object> map = new LinkedHashMap<>();
    profiles.forEach((k,v) -> {
      map.put(k,v.toData());
    });
    return map;
  }

}
