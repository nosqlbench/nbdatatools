package io.nosqlbench.nbdatatools.testdata;

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

import java.util.Arrays;

public class DummyFloatLayoutTest {

  @Test
  public void testDummyData() {
    DummyFloatLayout layout = DummyFloatLayout.forShape(10, 100, 10);
    System.out.println(layout);
    float[][] floats = layout.generateAll();
    for (float[] aFloat : floats) {
      System.out.println(Arrays.toString(aFloat));

    }
  }

}
