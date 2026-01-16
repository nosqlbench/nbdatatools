package io.nosqlbench.vshapes.extract;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link FitTableFormatter} ensuring correct column alignment.
 */
@Tag("unit")
public class FitTableFormatterTest {

    @Test
    void testBasicFormatting() {
        List<String> headers = List.of("normal", "beta", "uniform");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        formatter.addRow(0, new double[]{0.123, 0.456, 0.789}, 0);
        formatter.addRow(1, new double[]{0.234, 0.111, 0.890}, 1);

        String table = formatter.format();
        System.out.println("=== Basic Table ===");
        System.out.println(table);

        assertNotNull(table);
        assertTrue(table.contains("Dim"));
        assertTrue(table.contains("normal"));
        assertTrue(table.contains("beta"));
        assertTrue(table.contains("uniform"));
        assertTrue(table.contains("Best"));

        // Check alignment by verifying all lines have same length
        verifyAlignment(table);
    }

    @Test
    void testFormatScoreMethod() {
        // Test various score ranges - use exact values to avoid rounding ambiguity
        assertEquals("  0.123*", FitTableFormatter.formatScore(0.123, 8, true));
        assertEquals("  0.123 ", FitTableFormatter.formatScore(0.123, 8, false));

        assertEquals("  12.34*", FitTableFormatter.formatScore(12.34, 8, true));
        assertEquals("  12.34 ", FitTableFormatter.formatScore(12.34, 8, false));

        assertEquals("  123.4*", FitTableFormatter.formatScore(123.4, 8, true));
        assertEquals("  123.4 ", FitTableFormatter.formatScore(123.4, 8, false));

        assertEquals("   1234*", FitTableFormatter.formatScore(1234.0, 8, true));
        assertEquals("   1234 ", FitTableFormatter.formatScore(1234.0, 8, false));

        // Infinity/very large values
        assertEquals("    ---*", FitTableFormatter.formatScore(1e15, 8, true));
        assertEquals("    --- ", FitTableFormatter.formatScore(Double.MAX_VALUE, 8, false));

        // NaN
        assertEquals("    N/A*", FitTableFormatter.formatScore(Double.NaN, 8, true));
        assertEquals("    N/A ", FitTableFormatter.formatScore(Double.NaN, 8, false));

        // Infinity
        assertEquals("    N/A*", FitTableFormatter.formatScore(Double.POSITIVE_INFINITY, 8, true));

        System.out.println("formatScore tests passed");
    }

    @Test
    void testLongHeaderNames() {
        List<String> headers = List.of("inverse_gamma", "pearson_iv", "x");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        formatter.addRow(0, new double[]{0.5, 1.2, 0.3}, 2);
        formatter.addRow(1, new double[]{0.8, 0.9, 1.1}, 1);

        String table = formatter.format();
        System.out.println("=== Long Headers ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    @Test
    void testLargeDimensionNumbers() {
        List<String> headers = List.of("a", "b");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        formatter.addRow(0, new double[]{0.1, 0.2}, 0);
        formatter.addRow(99, new double[]{0.3, 0.4}, 1);
        formatter.addRow(999, new double[]{0.5, 0.6}, 0);

        String table = formatter.format();
        System.out.println("=== Large Dim Numbers ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    @Test
    void testMixedScoreRanges() {
        List<String> headers = List.of("col1", "col2", "col3", "col4");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        // Mix of small, medium, large, and invalid scores
        formatter.addRow(0, new double[]{0.001, 5.678, 123.4, 9999.0}, 0);
        formatter.addRow(1, new double[]{Double.NaN, 0.5, 1e15, 0.1}, 3);
        formatter.addRow(2, new double[]{Double.POSITIVE_INFINITY, 12.34, 56.78, 0.999}, 3);

        String table = formatter.format();
        System.out.println("=== Mixed Score Ranges ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    @Test
    void testEmptyRows() {
        List<String> headers = List.of("a", "b", "c");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        String table = formatter.format();
        System.out.println("=== Empty ===");
        System.out.println(table);

        assertEquals("No data\n", table);
    }

    @Test
    void testSingleColumn() {
        List<String> headers = List.of("only_one");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        formatter.addRow(0, new double[]{0.5}, 0);
        formatter.addRow(1, new double[]{0.7}, 0);

        String table = formatter.format();
        System.out.println("=== Single Column ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    @Test
    void testManyColumns() {
        List<String> headers = List.of("a", "b", "c", "d", "e", "f", "g", "h");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        formatter.addRow(0, new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}, 0);
        formatter.addRow(1, new double[]{0.9, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7}, 1);

        String table = formatter.format();
        System.out.println("=== Many Columns ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    @Test
    void testPadMethod() {
        assertEquals("   abc", FitTableFormatter.pad("abc", 6, true));
        assertEquals("abc   ", FitTableFormatter.pad("abc", 6, false));
        assertEquals("abc", FitTableFormatter.pad("abc", 3, true));
        assertEquals("abc", FitTableFormatter.pad("abc", 2, true));
    }

    @Test
    void testCenterPadMethod() {
        assertEquals(" abc ", FitTableFormatter.centerPad("abc", 5));
        assertEquals("  abc  ", FitTableFormatter.centerPad("abc", 7));
        assertEquals("abc", FitTableFormatter.centerPad("abc", 3));
        assertEquals("abc", FitTableFormatter.centerPad("abc", 2));
    }

    @Test
    void testInvalidRowThrows() {
        List<String> headers = List.of("a", "b");
        FitTableFormatter formatter = new FitTableFormatter(headers);

        assertThrows(IllegalArgumentException.class, () ->
            formatter.addRow(0, new double[]{1.0, 2.0, 3.0}, 0)
        );
    }

    @Test
    void testWithSparklines() {
        List<String> headers = List.of("normal", "uniform");
        FitTableFormatter formatter = new FitTableFormatter(headers, true);

        // Add rows with sparklines
        formatter.addRow(0, new double[]{0.123, 0.456}, 0, "▁▂▃▄▅▆▇█▇▅▃▁");
        formatter.addRow(1, new double[]{0.789, 0.234}, 1, "▄▄▄▄▄▄▄▄▄▄▄▄");

        String table = formatter.format();
        System.out.println("=== With Sparklines ===");
        System.out.println(table);

        // Verify sparkline column is present
        assertTrue(table.contains("Distribution"), "Should have Distribution header");
        assertTrue(table.contains("▁▂▃▄▅▆▇█▇▅▃▁"), "Should contain first sparkline");
        assertTrue(table.contains("▄▄▄▄▄▄▄▄▄▄▄▄"), "Should contain second sparkline");

        verifyAlignment(table);
    }

    @Test
    void testSparklinesDisabled() {
        List<String> headers = List.of("a", "b");
        FitTableFormatter formatter = new FitTableFormatter(headers, false);

        formatter.addRow(0, new double[]{0.1, 0.2}, 0, "▁▂▃▄▅▆▇█▇▅▃▁");

        String table = formatter.format();
        System.out.println("=== Sparklines Disabled ===");
        System.out.println(table);

        // Should not contain sparkline column
        assertFalse(table.contains("Distribution"), "Should not have Distribution header when disabled");

        verifyAlignment(table);
    }

    @Test
    void testMixedSparklineAndNull() {
        List<String> headers = List.of("normal", "beta");
        FitTableFormatter formatter = new FitTableFormatter(headers, true);

        formatter.addRow(0, new double[]{0.5, 0.6}, 0, "▇▇▇▆▅▄▃▂▁▁▁▁");
        formatter.addRow(1, new double[]{0.3, 0.4}, 0, null);  // null sparkline
        formatter.addRow(2, new double[]{0.7, 0.8}, 1, "");    // empty sparkline

        String table = formatter.format();
        System.out.println("=== Mixed Sparkline/Null ===");
        System.out.println(table);

        verifyAlignment(table);
    }

    /**
     * Verifies that all non-empty lines in the table have the same length.
     */
    private void verifyAlignment(String table) {
        String[] lines = table.split("\n");
        int expectedLength = -1;

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.isEmpty() || line.startsWith("...")) {
                continue;
            }

            if (expectedLength < 0) {
                expectedLength = line.length();
            } else {
                assertEquals(expectedLength, line.length(),
                    "Line " + i + " has different length.\nExpected: " + expectedLength +
                    "\nActual: " + line.length() +
                    "\nLine: '" + line + "'");
            }
        }
    }
}
