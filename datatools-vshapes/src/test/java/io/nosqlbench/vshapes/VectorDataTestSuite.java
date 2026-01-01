package io.nosqlbench.vshapes;

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

import io.nosqlbench.vshapes.extract.BestFitSelectorTest;
import io.nosqlbench.vshapes.extract.DatasetModelExtractorTest;
import io.nosqlbench.vshapes.extract.DimensionStatisticsTest;
import io.nosqlbench.vshapes.extract.ModelFitterTest;
import io.nosqlbench.vshapes.measures.HubnessMeasureTest;
import io.nosqlbench.vshapes.measures.LIDMeasureTest;
import io.nosqlbench.vshapes.measures.MarginMeasureTest;
import io.nosqlbench.vshapes.stream.AnalyzerHarnessTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
    // Core tests
    VectorUtilsTest.class,
    VectorSpaceAnalyzerTest.class,
    VectorSpaceAnalysisUtilsTest.class,
    AbstractAnalysisMeasureTest.class,
    TestVectorSpace.class,

    // Measures tests
    LIDMeasureTest.class,
    MarginMeasureTest.class,
    HubnessMeasureTest.class,

    // Extract package tests
    DimensionStatisticsTest.class,
    ModelFitterTest.class,
    BestFitSelectorTest.class,
    DatasetModelExtractorTest.class,

    // Stream package tests
    AnalyzerHarnessTest.class
})
public class VectorDataTestSuite {
}
