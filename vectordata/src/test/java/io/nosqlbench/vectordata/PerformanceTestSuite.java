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
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/// Performance test suite for the vectordata module.
///
/// This suite runs only the performance tests in the vectordata module with the JettyFileServerExtension applied.
/// Performance tests are identified by the @Tag("performance") annotation.
///
/// Like the main test suite, this suite uses the JettyFileServerExtension to provide a web server
/// that serves test resources for the tests in the vectordata module.
///
/// To run this suite, use one of the following Maven commands:
/// 1. mvn test -DskipPerformanceTests=false
/// 2. mvn test -Dtest=PerformanceTestSuite
@Suite
@SelectPackages("io.nosqlbench.vectordata")
@IncludeTags("performance")
@ExtendWith(JettyFileServerExtension.class)
public class PerformanceTestSuite {
    // This class is just a container for the suite annotations
}
