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

import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/// Accuracy test suite for the virtdata module.
///
/// This suite runs only the numerical accuracy tests in the virtdata module.
/// Accuracy tests are identified by the @Tag("accuracy") annotation.
///
/// ## Test Profile Usage
///
/// By default, only unit tests run. Use profiles to enable additional test categories:
///
/// | Command                              | Unit | Performance | Accuracy |
/// |--------------------------------------|------|-------------|----------|
/// | `mvn test`                           |  ✓   |             |          |
/// | `mvn test -Paccuracy`                |  ✓   |             |    ✓     |
/// | `mvn test -Pperformance`             |  ✓   |      ✓      |          |
/// | `mvn test -Pperformance,accuracy`    |  ✓   |      ✓      |    ✓     |
/// | `mvn test -Palltests`                |  ✓   |      ✓      |    ✓     |
@Suite
@SelectPackages("io.nosqlbench.datatools.virtdata")
@IncludeTags("accuracy")
public class AccuracyTestSuite {
    // This class is just a container for the suite annotations
}
