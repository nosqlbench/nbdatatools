package io.nosqlbench.vectordata.discovery;

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

import io.nosqlbench.vectordata.discovery.vector.FilesystemVectorTestDataView;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.downloader.VirtualVectorTestDataView;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the prebuffer() method in TestDataView interface and its implementations
public class TestDataViewPrebufferVectorTest {

    /// Test that the TestDataView interface has the prebuffer method
    @Test
    public void testTestDataViewInterfaceHasPrebufferMethod() throws NoSuchMethodException {
        Class<?> clazz = VectorTestDataView.class;

        // Test that prebuffer method exists
        Method prebufferMethod = clazz.getDeclaredMethod("prebuffer");
        assertNotNull(prebufferMethod, "prebuffer method should exist in TestDataView interface");
        assertEquals("java.util.concurrent.CompletableFuture", prebufferMethod.getReturnType().getName());
    }

    /// Test that FilesystemTestDataView implements the prebuffer method
    @Test
    public void testFilesystemTestDataViewHasPrebufferMethod() throws NoSuchMethodException {
        Class<?> clazz = FilesystemVectorTestDataView.class;

        // Test that prebuffer method exists
        Method prebufferMethod = clazz.getDeclaredMethod("prebuffer");
        assertNotNull(prebufferMethod, "prebuffer method should exist in FilesystemTestDataView");
        assertEquals("java.util.concurrent.CompletableFuture", prebufferMethod.getReturnType().getName());
    }

    /// Test that VirtualTestDataView implements the prebuffer method
    @Test
    public void testVirtualTestDataViewHasPrebufferMethod() throws NoSuchMethodException {
        Class<?> clazz = VirtualVectorTestDataView.class;

        // Test that prebuffer method exists
        Method prebufferMethod = clazz.getDeclaredMethod("prebuffer");
        assertNotNull(prebufferMethod, "prebuffer method should exist in VirtualTestDataView");
        assertEquals("java.util.concurrent.CompletableFuture", prebufferMethod.getReturnType().getName());
    }
}
