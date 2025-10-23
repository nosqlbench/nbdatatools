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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import io.nosqlbench.status.ConsoleProgressMode;
import picocli.CommandLine;

import java.util.Objects;

/**
 * Shared {@code --status} CLI option that controls the {@code nb.status.sink} system property
 * understood by {@link io.nosqlbench.status.StatusContext}. Commands using {@link io.nosqlbench.status.StatusContext}
 * should include this mixin to provide a uniform way to select console output behaviour.
 *
 * <p>The property is applied only when the user specifies {@code --status}. It is restored to its
 * previous value automatically when the returned {@link Scope} is closed.</p>
 */
public final class ConsoleProgressOption {

    private static final String PROP_KEY = "nb.status.sink";

    @CommandLine.Option(
        names = {"--status"},
        paramLabel = "MODE",
        description = {
            "Console status output mode. Valid values: ${COMPLETION-CANDIDATES}.",
            "Omit to inherit existing configuration or environment."
        },
        converter = ProgressModeConverter.class
    )
    private ConsoleProgressMode progressMode;

    /**
     * Applies the requested console status mode, returning a scope that restores the original
     * configuration when closed. If {@code --status} was not provided, this is a no-op scope.
     *
     * @return a scope that should be closed to restore prior configuration
     */
    public Scope scopedProperty() {
        if (progressMode == null) {
            return Scope.noop();
        }

        String desired = progressMode.getPropertyValue();
        String previous = System.getProperty(PROP_KEY);
        boolean hadPrevious = previous != null;

        if (!Objects.equals(previous, desired)) {
            System.setProperty(PROP_KEY, desired);
            return new Scope(true, hadPrevious, previous);
        }

        return new Scope(false, hadPrevious, previous);
    }

    /**
     * @return {@code true} if the user explicitly selected {@code --status=off}.
     */
    public boolean isExplicitlyOff() {
        return progressMode == ConsoleProgressMode.OFF;
    }

    /**
     * Exposed for testing and advanced callers that need to inspect the raw selection.
     *
     * @return the selected status mode, or {@code null} when not provided
     */
    public ConsoleProgressMode getProgressMode() {
        return progressMode;
    }

    /**
     * Applies the selected status mode without tracking previous values. Useful when callers manage
     * lifecycle elsewhere and only need to honor the user's explicit choice.
     */
    public void applyProgressMode() {
        if (progressMode != null) {
            System.setProperty(PROP_KEY, progressMode.getPropertyValue());
        }
    }

    /**
     * Picocli converter that maps user-provided values to {@link ConsoleProgressMode}.
     */
    public static final class ProgressModeConverter implements CommandLine.ITypeConverter<ConsoleProgressMode> {
        @Override
        public ConsoleProgressMode convert(String value) {
            try {
                return ConsoleProgressMode.fromString(value);
            } catch (IllegalArgumentException e) {
                throw new CommandLine.TypeConversionException(e.getMessage());
            }
        }
    }

    /**
     * Scope returned by {@link #scopedProperty()} that restores the prior system property when closed.
     */
    public static final class Scope implements AutoCloseable {
        private static final Scope NOOP = new Scope(false, false, null);

        private final boolean modified;
        private final boolean hadPrevious;
        private final String previousValue;

        private Scope(boolean modified, boolean hadPrevious, String previousValue) {
            this.modified = modified;
            this.hadPrevious = hadPrevious;
            this.previousValue = previousValue;
        }

        static Scope noop() {
            return NOOP;
        }

        @Override
        public void close() {
            if (!modified) {
                return;
            }
            if (hadPrevious) {
                System.setProperty(PROP_KEY, previousValue);
            } else {
                System.clearProperty(PROP_KEY);
            }
        }
    }
}
