package io.nosqlbench.nbvectors.jjq.apis;

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


import net.thisptr.jackson.jq.Function;
import net.thisptr.jackson.jq.Scope;

import java.util.List;
import java.util.Map;

public class NBJJQ {
  public static synchronized NBStateContext getContext(Scope scope) {
    Function nbs = scope.getFunction("nbstate", 0);
    if (nbs instanceof NBStateContextHolderHack nbsf) {
      return nbsf;
    } else {
      throw new RuntimeException("Missing state holder function");
    }

  }
  public static Map<String, Object> getState(Scope scope) {
    return getContext(scope).getState();
  }

  public synchronized static void register(NBBaseJQFunction f, Scope scope) {
    getContext(scope).register(f);
  }

  public static List<NBBaseJQFunction> getRegisteredFunctions(Scope scope) {
    return getContext(scope).getRegisteredFunctions();
  }

}
