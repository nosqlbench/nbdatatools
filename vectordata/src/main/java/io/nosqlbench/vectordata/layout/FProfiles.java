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


import io.nosqlbench.vectordata.internalapi.datasets.api.TestDataKind;

import java.util.LinkedHashMap;
import java.util.Map;

public record FProfiles(Map<String, FView> views) {

  public static FProfiles fromObject(Object v, FProfiles defaultProfile) {
    if (v instanceof FProfiles fp) {
      return fp;
    } else if (v instanceof Map<?, ?> pmap) {

      Object viewsObject = pmap.get("views");
      if (viewsObject instanceof Map<?, ?> vmap) {
        return fromObject(viewsObject, defaultProfile);
      }
      Map<String, FView> views = new LinkedHashMap<>();
      if (defaultProfile != null) {
        views.putAll(defaultProfile.views());
      }

      pmap.forEach((pk, pv) -> {
        String kindSpec = pk.toString();
        try {
          TestDataKind.fromString(kindSpec); // assert known type
          FView fview = FView.fromObject(pv);
          views.put(kindSpec, fview);
        } catch (Exception e) {
          throw new RuntimeException("invalid profile format for FView key[" + pk + "]:" + v, e);
        }
      });
      return new FProfiles(views);
    } else {
      throw new RuntimeException("invalid profile format:" + v);
    }
  }

  public Map<String,Object> toData() {
    Map<String,Object> map = new LinkedHashMap<>();
    views.forEach((k,v) -> {
      map.put(k,v.toData());
    });
    return map;
  }
}
