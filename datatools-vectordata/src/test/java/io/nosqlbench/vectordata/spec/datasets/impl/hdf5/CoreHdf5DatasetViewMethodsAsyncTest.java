package io.nosqlbench.vectordata.spec.datasets.impl.hdf5;

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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the async methods in CoreHdf5DatasetViewMethods
public class CoreHdf5DatasetViewMethodsAsyncTest {

    /// Test that async methods are present and properly declared
    @Test
    public void testAsyncMethodsExist() throws NoSuchMethodException {
        Class<?> clazz = CoreHdf5DatasetViewMethods.class;

        // Test that getAsync method exists
        Method getAsync = clazz.getDeclaredMethod("getAsync", long.class);
        assertNotNull(getAsync, "getAsync method should exist");
        assertEquals("java.util.concurrent.Future", getAsync.getReturnType().getName());

        // Test that getRangeAsync method exists
        Method getRangeAsync = clazz.getDeclaredMethod("getRangeAsync", long.class, long.class);
        assertNotNull(getRangeAsync, "getRangeAsync method should exist");
        assertEquals("java.util.concurrent.Future", getRangeAsync.getReturnType().getName());

        // Test that getIndexedAsync method exists
        Method getIndexedAsync = clazz.getDeclaredMethod("getIndexedAsync", long.class);
        assertNotNull(getIndexedAsync, "getIndexedAsync method should exist");
        assertEquals("java.util.concurrent.Future", getIndexedAsync.getReturnType().getName());

        // Test that getIndexedRangeAsync method exists
        Method getIndexedRangeAsync = clazz.getDeclaredMethod("getIndexedRangeAsync", long.class, long.class);
        assertNotNull(getIndexedRangeAsync, "getIndexedRangeAsync method should exist");
        assertEquals("java.util.concurrent.Future", getIndexedRangeAsync.getReturnType().getName());
    }
}