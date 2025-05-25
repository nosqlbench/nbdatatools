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

import io.nosqlbench.vectordata.downloader.testserver.TestWebServerExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/// Test suite for the vectordata module.
///
/// This suite runs all tests in the vectordata module with the TestWebServerExtension applied,
/// which ensures that a single TestWebServerFixture instance is started before any tests run
/// and stopped after all tests have completed.
///
/// The TestWebServerFixture provides a web server that serves test resources for the tests
/// in the vectordata module. By using a single instance for all tests, we avoid the overhead
/// of starting and stopping the server for each test class.
///
/// This suite is configured to include all tests in the vectordata module, so individual test
/// classes do not need to be annotated with @EnableTestWebServer to access the test web server.
/// The TestWebServerExtension is applied at the module level through this suite.
@Suite
@SelectPackages("io.nosqlbench.vectordata")
@ExtendWith(TestWebServerExtension.class)
public class VectorDataTestSuite {
    // This class is just a container for the suite annotations
}
