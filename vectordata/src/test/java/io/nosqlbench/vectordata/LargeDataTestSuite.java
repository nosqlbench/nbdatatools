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
import io.nosqlbench.vectordata.downloader.CatalogAccessLargefileTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/// Large data test suite for the vectordata module.
///
/// This suite runs only the large data tests in the vectordata module with the TestWebServerExtension applied.
/// Large data tests are identified by the @Tag("largedata") annotation.
///
/// Unlike the main test suite which uses @SelectPackages to discover tests, this suite uses @SelectClasses
/// to explicitly include the test classes tagged with "largedata". This approach ensures that tests in
/// subpackages are properly included in the test suite.
///
/// Like the main test suite, this suite uses the TestWebServerExtension to provide a web server
/// that serves test resources for the tests in the vectordata module.
///
/// To run this suite, use one of the following Maven commands:
/// 1. mvn test -DskipLargeDataTests=false
/// 2. mvn test -Dtest=LargeDataTestSuite
@Suite
@SelectClasses(CatalogAccessLargefileTest.class)
@IncludeTags("largedata")
@ExtendWith(JettyFileServerExtension.class)
public class LargeDataTestSuite {
    // This class is just a container for the suite annotations
}
