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


import java.nio.file.Path;

public class VectorTestData {

//  public static TestDataProfile lookup(String path, String profile) {
//    return lookup().find().lookup(name);
//  }
//

  public static TestDataGroup load(String path) {
    return load(Path.of(path));
  }

  public static TestDataGroup load(Path path) {
    return new TestDataGroup(path);
  }

  public static TestDataSources catalogs() {
    return new TestDataSources();
  }

  public static TestDataSources catalog(String url) {
    return TestDataSources.ofUrl(url);
  }

  public static TestDataSources lookup() {
    return TestDataSources.DEFAULT;
  }
}
