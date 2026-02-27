package io.nosqlbench.commands;

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

import io.nosqlbench.command.common.CompletionLineParser;
import picocli.AutoComplete;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/// Dynamic completion hook for bash `complete -C`.
@CommandLine.Command(name = "complete", hidden = true, description = "Generate dynamic completion candidates.")
public class NbvectorsComplete implements Runnable {

    /// Creates a new NbvectorsComplete command.
    public NbvectorsComplete() {}

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Parameters(arity = "0..3", hidden = true)
    private List<String> bashArgs = new ArrayList<>();

    @Override
    public void run() {
        Optional<CompletionLineParser.ParsedLine> parsed = CompletionLineParser.parseFromEnv();
        if (parsed.isEmpty()) {
            return;
        }

        CompletionLineParser.ParsedLine line = parsed.get();
        List<String> args = new ArrayList<>(line.args());
        int argIndex = line.argIndex();

        String invokedName = bashArgs.isEmpty() ? null : bashArgs.get(0);
        if (!args.isEmpty() && isCommandToken(args.get(0), invokedName, spec.root().name())) {
            if (argIndex == 0) {
                return;
            }
            args.remove(0);
            argIndex -= 1;
        }

        if (argIndex < 0) {
            return;
        }

        List<CharSequence> candidates = new ArrayList<>();
        AutoComplete.complete(spec.root(),
            args.toArray(new String[0]),
            argIndex,
            line.positionInArg(),
            line.positionInArg(),
            candidates);

        String prefix = line.currentArgPrefix();
        String normalizedPrefix = normalizePrefix(prefix);
        List<String> suffixCandidates = new ArrayList<>(candidates.size());
        for (CharSequence candidate : candidates) {
            String value = candidate.toString();
            if (!prefix.isEmpty() && value.startsWith(prefix)) {
                value = value.substring(prefix.length());
            } else if (!normalizedPrefix.isEmpty() && value.startsWith(normalizedPrefix)) {
                value = value.substring(normalizedPrefix.length());
            }
            suffixCandidates.add(value);
        }
        List<String> fullCandidates = new ArrayList<>(candidates.size());
        for (CharSequence candidate : candidates) {
            String value = candidate.toString();
            if (prefix.isEmpty()) {
                fullCandidates.add(value);
            } else if (value.startsWith(prefix)) {
                fullCandidates.add(value);
            } else if (!normalizedPrefix.isEmpty() && value.startsWith(normalizedPrefix)) {
                fullCandidates.add(value);
            } else {
                fullCandidates.add(prefix + value);
            }
        }

        PrintWriter out = spec.commandLine().getOut();
        int compType = readCompType();
        boolean listRequest = isListRequest(compType);
        if (fullCandidates.size() == 1) {
            String only = fullCandidates.get(0);
            if (shouldAppendSpace(only)) {
                out.println(only + " ");
            } else {
                out.println(only);
            }
            out.flush();
            return;
        }
        if (!listRequest) {
            String common = longestCommonPrefix(fullCandidates);
            if (common.length() > prefix.length()) {
                out.println(common);
            }
            out.flush();
            return;
        }
        for (String candidate : suffixCandidates) {
            if (!candidate.isEmpty()) {
                out.println(candidate);
            }
        }
        out.flush();
    }

    private static int readCompType() {
        String raw = System.getenv("COMP_TYPE");
        if (raw == null || raw.isBlank()) {
            return -1;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static boolean isListRequest(int compType) {
        return compType == 63 || compType == 33 || compType == 64;
    }

    private static boolean shouldAppendSpace(String candidate) {
        if (candidate.isEmpty()) {
            return false;
        }
        if (candidate.endsWith(" ") || candidate.endsWith(".") || candidate.endsWith(":")
            || candidate.endsWith("=") || candidate.endsWith("/") || candidate.endsWith("\\")) {
            return false;
        }
        return true;
    }

    private static String longestCommonPrefix(List<String> values) {
        if (values.isEmpty()) {
            return "";
        }
        String prefix = values.get(0);
        for (int i = 1; i < values.size(); i++) {
            String value = values.get(i);
            int max = Math.min(prefix.length(), value.length());
            int j = 0;
            while (j < max && prefix.charAt(j) == value.charAt(j)) {
                j++;
            }
            prefix = prefix.substring(0, j);
            if (prefix.isEmpty()) {
                break;
            }
        }
        return prefix;
    }

    private static boolean isCommandToken(String token, String invokedName, String commandName) {
        if (matchesCommandToken(token, invokedName) || matchesCommandToken(token, commandName)) {
            return true;
        }
        return false;
    }

    private static boolean matchesCommandToken(String token, String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (token.equals(name)) {
            return true;
        }
        try {
            Path path = Path.of(token);
            Path fileName = path.getFileName();
            return fileName != null && fileName.toString().equals(name);
        } catch (InvalidPathException e) {
            return false;
        }
    }

    private static String normalizePrefix(String prefix) {
        if (prefix == null || prefix.isEmpty() || prefix.indexOf(':') < 0) {
            return prefix == null ? "" : prefix;
        }
        return prefix.replace(':', '.');
    }
}
