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


import io.nosqlbench.vectordata.TestDataGroup;
import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.internalapi.datasets.views.BaseVectors;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborIndices;
import org.assertj.core.util.URLs;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class HistoricFormatTest {

  @Disabled
  @Test
  public void testFullConfig() {
    URL resource = getClass().getClassLoader().getResource("glove-25-angular.hdf5");

    TestDataGroup datagroup = new TestDataGroup(Path.of(resource.getPath()));
    TestDataView dataview = datagroup.getDefaultProfile();
    BaseVectors baseV =
        dataview.getBaseVectors().orElseThrow(() -> new RuntimeException("base vectors not found"));
    float[] floats = baseV.get(23);
    System.out.println(Arrays.toString(floats));

    NeighborIndices indices = dataview.getNeighborIndices().orElseThrow(() -> new RuntimeException("indices not found"));
    int[] ints = indices.get(34);
    System.out.println(ints);

  }

}
