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

package io.nosqlbench.command.common;

import io.nosqlbench.status.ConsoleProgressMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsoleProgressOptionTest {

    private static final String PROP_KEY = "nb.status.sink";

    @AfterEach
    void clearProperty() {
        System.clearProperty(PROP_KEY);
    }

    @Test
    void scopeSetsAndRestoresProperty() {
        DummyCommand command = new DummyCommand();
        new CommandLine(command).parseArgs("--status", "panel");

        assertNull(System.getProperty(PROP_KEY), "precondition: no status property set");

        try (ConsoleProgressOption.Scope scope = command.option.scopedProperty()) {
            assertEquals("panel", System.getProperty(PROP_KEY));
        }

        assertNull(System.getProperty(PROP_KEY), "scope should restore system property");
    }

    @Test
    void converterAcceptsSynonyms() {
        DummyCommand command = new DummyCommand();
        new CommandLine(command).parseArgs("--status", "logger");

        assertEquals(ConsoleProgressMode.LOG, command.option.getProgressMode());

        try (ConsoleProgressOption.Scope scope = command.option.scopedProperty()) {
            assertEquals("log", System.getProperty(PROP_KEY));
        }
    }

    @Test
    void offModeIsReported() {
        DummyCommand command = new DummyCommand();
        new CommandLine(command).parseArgs("--status", "off");

        assertTrue(command.option.isExplicitlyOff());

        try (ConsoleProgressOption.Scope scope = command.option.scopedProperty()) {
            assertEquals("off", System.getProperty(PROP_KEY));
        }
    }

    @Test
    void noOptionLeavesPropertyUntouched() {
        DummyCommand command = new DummyCommand();
        new CommandLine(command).parseArgs();

        try (ConsoleProgressOption.Scope scope = command.option.scopedProperty()) {
            assertNull(System.getProperty(PROP_KEY));
        }
    }

    @Test
    void applyProgressModeSetsPropertyWithoutScope() {
        DummyCommand command = new DummyCommand();
        new CommandLine(command).parseArgs("--status", "panel");

        command.option.applyProgressMode();
        assertEquals("panel", System.getProperty(PROP_KEY));
    }

    private static final class DummyCommand implements Runnable {
        @CommandLine.Mixin
        final ConsoleProgressOption option = new ConsoleProgressOption();

        @Override
        public void run() {
        }
    }
}
