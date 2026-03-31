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
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/// Lists all namespaces in a slabtastic file.
///
/// For each namespace, reports the index, name, page count, record count,
/// and ordinal range. Single-namespace files show just the default namespace.
@CommandLine.Command(
    name = "namespaces",
    header = "List namespaces in a slab file",
    description = "Lists all namespaces with their index, name, page count, record count, and ordinal range.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_namespaces implements Callable<Integer> {

    /// Creates a new instance of the namespaces subcommand.
    public CMD_slab_namespaces() {}

    @CommandLine.Parameters(index = "0", description = "Path to the slabtastic file")
    private Path file;

    @Override
    public Integer call() {
        PrintStream out = System.out;
        try (SlabReader reader = new SlabReader(file)) {
            out.println("File: " + file);
            out.println();

            out.printf("%-8s  %-20s  %-8s  %-12s  %-30s%n",
                "Index", "Name", "Pages", "Records", "Ordinal Range");
            out.println("-".repeat(82));

            for (String ns : reader.namespaces()) {
                List<SlabReader.PageSummary> pages = reader.pages(ns);
                long recordCount = reader.recordCount(ns);
                int pageCount = reader.pageCount(ns);

                String displayName = ns.isEmpty() ? "(default)" : ns;
                String ordinalRange = "N/A";
                if (!pages.isEmpty()) {
                    long minOrd = pages.getFirst().startOrdinal();
                    SlabReader.PageSummary last = pages.getLast();
                    long maxOrd = last.startOrdinal() + last.recordCount() - 1;
                    ordinalRange = "[%d, %d]".formatted(minOrd, maxOrd);
                }

                // We don't have direct access to the namespace index from the reader
                // but we can report the namespace data
                out.printf("%-8s  %-20s  %-8d  %-12d  %-30s%n",
                    "-", displayName, pageCount, recordCount, ordinalRange);
            }

            out.println();
            out.printf("Total namespaces: %d%n", reader.namespaces().size());
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
