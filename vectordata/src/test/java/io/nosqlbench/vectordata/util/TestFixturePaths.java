package io.nosqlbench.vectordata.util;

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
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/// Utility class for managing test-specific paths to ensure test isolation.
/// 
/// This class ensures that each test class and method gets its own unique
/// subdirectory under the shared JettyFileServerExtension temp directory,
/// preventing conflicts between concurrent tests while maintaining the
/// single shared server instance.
public class TestFixturePaths {
    
    private static final AtomicLong uniqueCounter = new AtomicLong(System.currentTimeMillis());
    
    /// Creates a test-specific subdirectory under the shared server's temp directory.
    /// 
    /// The directory structure will be:
    /// temp/{TestClassName}/{methodName}_{uniqueId}/
    /// 
    /// @param testInfo JUnit TestInfo containing class and method names
    /// @return Path to the unique test directory
    /// @throws IOException if directory creation fails
    public static Path createTestSpecificTempDir(TestInfo testInfo) throws IOException {
        String className = testInfo.getTestClass()
                                  .map(Class::getSimpleName)
                                  .orElse("UnknownTest");
        String methodName = testInfo.getTestMethod()
                                   .map(java.lang.reflect.Method::getName)
                                   .orElse("unknownMethod");
        
        long uniqueId = uniqueCounter.incrementAndGet();
        
        Path testDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT
                                             .resolve(className)
                                             .resolve(methodName + "_" + uniqueId);
        
        Files.createDirectories(testDir);
        return testDir;
    }
    
    /// Creates a unique filename with test class and method prefix.
    /// 
    /// Format: {TestClassName}_{methodName}_{basename}
    /// 
    /// @param testInfo JUnit TestInfo containing class and method names
    /// @param basename The base filename (e.g., "cache.dat")
    /// @return Unique filename string
    public static String createTestSpecificFilename(TestInfo testInfo, String basename) {
        String className = testInfo.getTestClass()
                                  .map(Class::getSimpleName)
                                  .orElse("UnknownTest");
        String methodName = testInfo.getTestMethod()
                                   .map(java.lang.reflect.Method::getName)
                                   .orElse("unknownMethod");
        
        return className + "_" + methodName + "_" + basename;
    }
    
    /// Creates a URL for a test-specific file on the shared server.
    /// 
    /// This creates a URL pointing to temp/{TestClassName}/{methodName}_{uniqueId}/{filename}
    /// on the shared JettyFileServerExtension server.
    /// 
    /// @param testInfo JUnit TestInfo containing class and method names
    /// @param filename The filename within the test directory
    /// @return URL pointing to the test-specific file on the server
    public static URL createTestSpecificServerUrl(TestInfo testInfo, String filename) {
        String className = testInfo.getTestClass()
                                  .map(Class::getSimpleName)
                                  .orElse("UnknownTest");
        String methodName = testInfo.getTestMethod()
                                   .map(java.lang.reflect.Method::getName)
                                   .orElse("unknownMethod");
        
        long uniqueId = uniqueCounter.get();
        
        try {
            URL baseUrl = JettyFileServerExtension.getBaseUrl();
            String testPath = "temp/" + className + "/" + methodName + "_" + uniqueId + "/" + filename;
            return new URL(baseUrl, testPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test-specific server URL", e);
        }
    }
    
    /// Creates a test-specific resource path within the rawdatasets directory.
    /// 
    /// This is for tests that need to access static test resources but want
    /// to ensure they don't conflict with other tests accessing the same resource.
    /// 
    /// @param resourcePath The path within rawdatasets (e.g., "testxvec/testxvec_base.fvec")
    /// @return URL pointing to the resource on the shared server
    public static URL createResourceUrl(String resourcePath) {
        try {
            URL baseUrl = JettyFileServerExtension.getBaseUrl();
            return new URL(baseUrl, "rawdatasets/" + resourcePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create resource URL for: " + resourcePath, e);
        }
    }
    
    /// Cleans up a test-specific directory after test completion.
    /// 
    /// This is typically called in test cleanup methods to remove
    /// test artifacts and prevent accumulation of temp files.
    /// 
    /// @param testDir The test directory to clean up
    public static void cleanupTestDir(Path testDir) {
        if (testDir != null && Files.exists(testDir)) {
            try {
                Files.walk(testDir)
                     .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                     .forEach(path -> {
                         try {
                             Files.deleteIfExists(path);
                         } catch (IOException e) {
                             // Log but don't fail - cleanup is best effort
                             System.err.println("Warning: Could not delete test file: " + path + " - " + e.getMessage());
                         }
                     });
            } catch (IOException e) {
                System.err.println("Warning: Could not clean up test directory: " + testDir + " - " + e.getMessage());
            }
        }
    }
}