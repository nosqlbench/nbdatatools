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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/// Parses COMP_LINE/COMP_POINT for completion contexts.
public final class CompletionLineParser {

    private CompletionLineParser() {
    }

    public record ParsedLine(List<String> args, int argIndex, int positionInArg, String currentArgPrefix) {
    }

    public static Optional<ParsedLine> parseFromEnv() {
        String line = System.getenv("COMP_LINE");
        String point = System.getenv("COMP_POINT");
        if (line == null || point == null) {
            return Optional.empty();
        }
        int cursor;
        try {
            cursor = Integer.parseInt(point);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(parse(line, cursor));
    }

    public static ParsedLine parse(String line, int cursor) {
        String safeLine = line == null ? "" : line;
        int safeCursor = Math.max(0, Math.min(cursor, safeLine.length()));
        List<String> prefixArgs = splitCommandLine(safeLine.substring(0, safeCursor), true);
        String currentPrefix = prefixArgs.isEmpty() ? "" : prefixArgs.get(prefixArgs.size() - 1);
        int argIndex = prefixArgs.isEmpty() ? 0 : prefixArgs.size() - 1;
        int positionInArg = currentPrefix.length();
        List<String> args = splitCommandLine(safeLine, false);
        return new ParsedLine(args, argIndex, positionInArg, currentPrefix);
    }

    public static List<String> splitCommandLine(String line, boolean includeTrailingEmpty) {
        List<String> args = new ArrayList<>();
        if (line == null) {
            if (includeTrailingEmpty) {
                args.add("");
            }
            return args;
        }

        StringBuilder current = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean escape = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (escape) {
                current.append(ch);
                escape = false;
                continue;
            }
            if (!inSingle && ch == '\\') {
                escape = true;
                continue;
            }
            if (!inDouble && ch == '\'') {
                inSingle = !inSingle;
                continue;
            }
            if (!inSingle && ch == '"') {
                inDouble = !inDouble;
                continue;
            }
            if (!inSingle && !inDouble && Character.isWhitespace(ch)) {
                if (current.length() > 0) {
                    args.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(ch);
        }

        if (escape) {
            current.append('\\');
        }

        if (current.length() > 0) {
            args.add(current.toString());
            return args;
        }

        if (includeTrailingEmpty) {
            if (line.isEmpty()) {
                args.add("");
            } else if (!inSingle && !inDouble && Character.isWhitespace(line.charAt(line.length() - 1))) {
                args.add("");
            }
        }

        return args;
    }
}
