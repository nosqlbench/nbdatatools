package io.nosqlbench.vectordata.merklev2;

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

import io.nosqlbench.vectordata.merklev2.schedulers.AggressiveChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.DefaultChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.ConservativeChunkScheduler;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for MAFileChannel scheduler switching during prebuffer operations
public class MAFileChannelSchedulerSwitchTest {

    /// Test that MAFileChannel has the required scheduler methods
    @Test
    public void testMAFileChannelHasSchedulerMethods() throws NoSuchMethodException {
        Class<?> clazz = MAFileChannel.class;

        // Test that getChunkScheduler method exists
        Method getSchedulerMethod = clazz.getDeclaredMethod("getChunkScheduler");
        assertNotNull(getSchedulerMethod, "getChunkScheduler method should exist");
        assertEquals(ChunkScheduler.class, getSchedulerMethod.getReturnType());

        // Test that setChunkScheduler method exists
        Method setSchedulerMethod = clazz.getDeclaredMethod("setChunkScheduler", ChunkScheduler.class);
        assertNotNull(setSchedulerMethod, "setChunkScheduler method should exist");

        // Test that prebuffer method exists
        Method prebufferMethod = clazz.getDeclaredMethod("prebuffer", long.class, long.class);
        assertNotNull(prebufferMethod, "prebuffer method should exist");
        assertEquals("java.util.concurrent.CompletableFuture", prebufferMethod.getReturnType().getName());
    }

    /// Test that scheduler classes exist and can be instantiated
    @Test
    public void testSchedulerClassesExist() {
        // Test that AggressiveChunkScheduler can be instantiated
        AggressiveChunkScheduler aggressiveScheduler = new AggressiveChunkScheduler();
        assertNotNull(aggressiveScheduler, "AggressiveChunkScheduler should be instantiable");
        assertTrue(aggressiveScheduler instanceof ChunkScheduler, 
            "AggressiveChunkScheduler should implement ChunkScheduler");

        // Test that DefaultChunkScheduler can be instantiated
        DefaultChunkScheduler defaultScheduler = new DefaultChunkScheduler();
        assertNotNull(defaultScheduler, "DefaultChunkScheduler should be instantiable");
        assertTrue(defaultScheduler instanceof ChunkScheduler, 
            "DefaultChunkScheduler should implement ChunkScheduler");

        // Test that ConservativeChunkScheduler can be instantiated
        ConservativeChunkScheduler conservativeScheduler = new ConservativeChunkScheduler();
        assertNotNull(conservativeScheduler, "ConservativeChunkScheduler should be instantiable");
        assertTrue(conservativeScheduler instanceof ChunkScheduler, 
            "ConservativeChunkScheduler should implement ChunkScheduler");
    }
}