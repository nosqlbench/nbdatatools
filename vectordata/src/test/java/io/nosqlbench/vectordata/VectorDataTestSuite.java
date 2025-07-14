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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.suite.api.ExcludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/// Test suite for the vectordata module.
///
/// This suite runs all tests in the vectordata module with the TestWebServerExtension applied,
/// which ensures that a single web server fixture instance is started before any tests run
/// and stopped after all tests have completed.
///
/// The TestWebServerExtension uses the JettyFileServerFixture implementation:
/// JettyFileServerFixture - Uses Eclipse Jetty
///
/// Previously, the TestWebServerExtension supported two different web server implementations,
/// but now it only uses JettyFileServerFixture.
///
/// The web server fixture provides a web server that serves test resources for the tests
/// in the vectordata module. By using a single instance for all tests, we avoid the overhead
/// of starting and stopping the server for each test class.
///
/// This suite is configured to include all tests in the vectordata module except performance tests,
/// which are run separately by the PerformanceTestSuite. Performance tests are identified by the
/// @Tag("performance") annotation.
///
/// Individual test classes do not need to be annotated with any special annotation to access the
/// test web server. The TestWebServerExtension is applied at the module level through this suite.
@Suite
@SelectPackages("io.nosqlbench.vectordata")
@ExcludeTags({"performance","largedata"})
@ExtendWith(JettyFileServerExtension.class)
public class VectorDataTestSuite {
    // This class is just a container for the suite annotations
}
