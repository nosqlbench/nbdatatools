package io.nosqlbench.datatools.virtdata;

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

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/// Test suite for all virtdata vector generation tests.
/// This suite runs all unit tests for the vector generation functionality.
@Suite
@SelectClasses({
    GaussianComponentModelTest.class,
    VectorSpaceModelTest.class,
    InverseGaussianCDFTest.class,
    TruncatedGaussianSamplerTest.class,
    StratifiedSamplerTest.class,
    VectorGenTest.class,
    VectorGenFactoryTest.class,
    VectorSpaceModelConfigTest.class
})
public class VectorDataTestSuite {
}
