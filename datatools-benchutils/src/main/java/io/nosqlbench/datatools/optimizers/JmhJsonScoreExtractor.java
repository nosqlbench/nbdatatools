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

package io.nosqlbench.datatools.optimizers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Extracts the throughput score from a JMH JSON result file.
///
/// JMH's `-rf json` output is an array of result objects, each containing
/// a `primaryMetric` with a `score` field. This utility prints the score
/// from the first result as a long integer (truncated, no decimals).
///
/// Usage: `java -cp benchmarks.jar ...JmhJsonScoreExtractor <json-file>`
public class JmhJsonScoreExtractor {

    private JmhJsonScoreExtractor() {}

    /// Entry point for standalone extraction.
    /// @param args command-line arguments; first argument is the JSON file path
    /// @throws IOException if the file cannot be read
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: JmhJsonScoreExtractor <json-file>");
            System.exit(1);
        }
        double score = extractScore(Path.of(args[0]));
        if (score >= 0) {
            System.out.printf("%.0f%n", score);
        } else {
            System.exit(1);
        }
    }

    /// Parses the throughput score from JMH's JSON result file.
    ///
    /// Looks for `"score"` within the `"primaryMetric"` section of the
    /// first result object in the JSON array.
    /// @param jsonFile the JMH JSON result file
    /// @return the extracted score, or -1 if not found
    /// @throws IOException if the file cannot be read
    public static double extractScore(Path jsonFile) throws IOException {
        String json = Files.readString(jsonFile);
        int pmIdx = json.indexOf("\"primaryMetric\"");
        if (pmIdx < 0) return -1;
        String afterPm = json.substring(pmIdx);
        Matcher m = Pattern.compile("\"score\"\\s*:\\s*([\\d.E+-]+)").matcher(afterPm);
        if (m.find()) return Double.parseDouble(m.group(1));
        return -1;
    }
}
