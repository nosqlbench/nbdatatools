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

package io.nosqlbench.slabtastic.cli;

import io.nosqlbench.slabtastic.SlabReader;

import java.util.Set;
import java.util.stream.Collectors;

/// Resolves the effective namespace for CLI commands operating on
/// slab files.
///
/// Per the slabtastic spec, commands should not assume any particular
/// set of namespaces will be present. When a file has multiple
/// namespaces and the user has not specified one, the command should
/// either operate on all namespaces or prompt the user to choose.
public final class NamespaceResolver {

    private NamespaceResolver() {}

    /// Returns the non-empty (user-visible) namespaces in the file.
    ///
    /// The default namespace `""` is excluded from the result since
    /// it is always present structurally but may contain no data.
    ///
    /// @param reader the slab reader
    /// @return the set of named (non-default) namespaces
    public static Set<String> namedNamespaces(SlabReader reader) {
        return reader.namespaces().stream()
            .filter(ns -> !ns.isEmpty())
            .collect(Collectors.toSet());
    }

    /// Resolves the namespace to operate on for a read command.
    ///
    /// If the user specified a namespace (non-empty), returns it.
    /// If the user did not specify a namespace (empty string) and the
    /// file has only the default namespace, returns `""`.
    /// If the user did not specify a namespace and the file has named
    /// namespaces, returns null — the caller should print an error
    /// with the available namespaces listed via
    /// {@link #formatNamespaceHint(SlabReader)}.
    ///
    /// @param userNamespace the namespace from the CLI option (empty
    ///                      string if unspecified)
    /// @param reader        the slab reader
    /// @return the resolved namespace, or null if the user must choose
    public static String resolveForRead(String userNamespace, SlabReader reader) {
        if (!userNamespace.isEmpty()) {
            return userNamespace;
        }
        Set<String> named = namedNamespaces(reader);
        if (named.isEmpty()) {
            return "";
        }
        return null;
    }

    /// Formats a hint message listing available namespaces.
    ///
    /// @param reader the slab reader
    /// @return a formatted hint string
    public static String formatNamespaceHint(SlabReader reader) {
        Set<String> named = namedNamespaces(reader);
        return "This file contains " + named.size() + " namespace(s): "
            + named.stream().sorted().collect(Collectors.joining(", "))
            + "\nUse -n <namespace> to specify which namespace to operate on.";
    }
}
