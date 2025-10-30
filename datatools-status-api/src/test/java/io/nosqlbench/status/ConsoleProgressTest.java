/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.status;

import io.nosqlbench.status.eventing.StatusSink;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.sinks.NoopStatusSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsoleProgressTest {

    private static final String PROP_KEY = "nb.status.sink";

    @AfterEach
    void clearProperty() {
        System.clearProperty(PROP_KEY);
    }

    @Test
    void autoAttachSkipsWhenPreconfiguredSinks() {
        StatusSink sentinel = NoopStatusSink.getInstance();
        try (StatusContext context = new StatusContext("preconfigured", List.of(sentinel))) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "existing sinks should be preserved");
            assertSame(sentinel, sinks.get(0), "explicit sink should remain untouched");
        }
    }

    @Test
    void logModeInstallsConsoleLoggerSink() {
        System.setProperty(PROP_KEY, "log");
        try (StatusContext context = new StatusContext("log-mode")) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "logger mode should install exactly one sink");
            assertTrue(sinks.get(0) instanceof ConsoleLoggerSink,
                "logger mode should install ConsoleLoggerSink");
        }
    }

    @Test
    void offModeInstallsNoSinks() {
        System.setProperty(PROP_KEY, "off");
        try (StatusContext context = new StatusContext("off-mode")) {
            assertTrue(context.getSinks().isEmpty(), "off mode should leave context without sinks");
        }
    }

    @Test
    void invalidModeFallsBackToDefaultLoggerWhenNoConsole() {
        Assumptions.assumeTrue(System.console() == null,
            "test requires headless environment so default mode resolves to logger");
        System.setProperty(PROP_KEY, "invalid-mode");
        try (StatusContext context = new StatusContext("fallback-mode")) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "invalid mode should still install a fallback sink");
            assertTrue(sinks.get(0) instanceof ConsoleLoggerSink,
                "headless fallback should use ConsoleLoggerSink");
        }
    }

    @Test
    void explicitProgressModeTakesPrecedenceOverSystemProperty() {
        System.setProperty(PROP_KEY, "log");
        try (StatusContext context = new StatusContext("explicit-off", Optional.of(StatusSinkMode.OFF))) {
            assertTrue(context.getSinks().isEmpty(),
                "explicit OFF mode should override system property 'log'");
        }
    }

    @Test
    void explicitLogModeInstallsConsoleLoggerSink() {
        try (StatusContext context = new StatusContext("explicit-log", Optional.of(StatusSinkMode.LOG))) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "explicit LOG mode should install exactly one sink");
            assertTrue(sinks.get(0) instanceof ConsoleLoggerSink,
                "explicit LOG mode should install ConsoleLoggerSink");
        }
    }

    @Test
    void explicitOffModeInstallsNoSinks() {
        try (StatusContext context = new StatusContext("explicit-off", Optional.of(StatusSinkMode.OFF))) {
            assertTrue(context.getSinks().isEmpty(),
                "explicit OFF mode should leave context without sinks");
        }
    }

    @Test
    void explicitAutoModeWithNoConsoleInstallsLogger() {
        Assumptions.assumeTrue(System.console() == null,
            "test requires headless environment");
        try (StatusContext context = new StatusContext("explicit-auto", Optional.of(StatusSinkMode.AUTO))) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "explicit AUTO mode should install exactly one sink");
            assertTrue(sinks.get(0) instanceof ConsoleLoggerSink,
                "explicit AUTO mode in headless env should install ConsoleLoggerSink");
        }
    }

    @Test
    void emptyOptionalUsesSystemProperty() {
        System.setProperty(PROP_KEY, "log");
        try (StatusContext context = new StatusContext("empty-optional", Optional.empty())) {
            List<StatusSink> sinks = context.getSinks();
            assertEquals(1, sinks.size(), "empty Optional should fall back to system property");
            assertTrue(sinks.get(0) instanceof ConsoleLoggerSink,
                "system property 'log' should install ConsoleLoggerSink");
        }
    }
}
