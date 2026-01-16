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

import io.nosqlbench.vshapes.TerminalTestRig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for ExploreRenderer using TerminalTestRig for headless terminal emulation.
///
/// These tests verify that the explore command's display layout renders correctly
/// in a real terminal emulator.
class ExploreRendererTest {

    /// Tests basic layout with a single dimension.
    @Test
    void testSingleDimensionLayout() throws IOException {
        // Create renderer with 40 columns, 10 rows
        ExploreRenderer renderer = new ExploreRenderer(40, 10);

        // Create histogram data for dimension 0 (uniform distribution)
        int[] counts = new int[renderer.getBins()];
        Arrays.fill(counts, 100);

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("test.fvec")
            .vectorCount(10000)
            .fileDimensions(128)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(10000)
            .targetSamples(10000)
            .build();

        // Render the frame
        String frame = renderer.renderFrame(state);

        // Create terminal with enough space (80 cols, 30 rows)
        TerminalTestRig rig = new TerminalTestRig(80, 30);
        rig.feedFrame(frame);

        // Take snapshot
        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Verify key layout elements
        assertNotNull(screen, "Screen should not be null");
        assertTrue(screen.contains("VECTOR EXPLORER"), "Should contain title");
        assertTrue(screen.contains("test.fvec"), "Should contain filename");
        assertTrue(screen.contains("10,000 vectors"), "Should contain vector count");
        assertTrue(screen.contains("128 dimensions"), "Should contain dimension count");
        assertTrue(screen.contains("Current: dim 0"), "Should show current dimension");
        assertTrue(screen.contains("Dimension 0"), "Legend should show dimension 0");
        assertTrue(screen.contains("←/→: dim"), "Should show controls");

        // Verify histogram axis labels
        assertTrue(screen.contains("1.0"), "Should show Y-axis max label");
        assertTrue(screen.contains("0.0"), "Should show Y-axis min label");
        assertTrue(screen.contains("-1.000"), "Should show X-axis min label");
        assertTrue(screen.contains("1.000"), "Should show X-axis max label");
    }

    /// Tests layout with multiple overlaid dimensions.
    @Test
    void testMultipleDimensionOverlay() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(40, 10);

        // Create histogram data for multiple dimensions
        int[] counts0 = new int[renderer.getBins()];
        int[] counts1 = new int[renderer.getBins()];
        int[] counts2 = new int[renderer.getBins()];

        // Dim 0: left-skewed
        for (int i = 0; i < renderer.getBins(); i++) {
            counts0[i] = Math.max(0, renderer.getBins() - i);
        }

        // Dim 1: right-skewed
        for (int i = 0; i < renderer.getBins(); i++) {
            counts1[i] = i;
        }

        // Dim 2: centered (normal-ish)
        int center = renderer.getBins() / 2;
        for (int i = 0; i < renderer.getBins(); i++) {
            int dist = Math.abs(i - center);
            counts2[i] = Math.max(0, center - dist);
        }

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);
        enabledDims.add(1);
        enabledDims.add(2);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts0);
        histogramCounts.put(1, counts1);
        histogramCounts.put(2, counts2);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("multi.fvec")
            .vectorCount(50000)
            .fileDimensions(256)
            .currentDimension(1)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-2.0f)
            .globalMax(2.0f)
            .samplesLoaded(5000)
            .targetSamples(10000)
            .build();

        String frame = renderer.renderFrame(state);

        // Use a larger terminal to ensure everything fits
        TerminalTestRig rig = new TerminalTestRig(100, 40);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Debug output for failures
        if (!screen.contains("Dimension 2")) {
            System.err.println("DEBUG: Screen content:\n" + screen);
            System.err.println("DEBUG: Frame content:\n" + frame);
        }

        // Verify multi-dimension display
        assertTrue(screen.contains("3 dims:"), "Should show '3 dims:' for multiple dimensions. Screen:\n" + screen);
        assertTrue(screen.contains("Dimension 0"), "Legend should show dimension 0. Screen:\n" + screen);
        assertTrue(screen.contains("Dimension 1"), "Legend should show dimension 1. Screen:\n" + screen);
        assertTrue(screen.contains("Dimension 2"), "Legend should show dimension 2. Screen:\n" + screen);
        assertTrue(screen.contains("Current: dim 1"), "Should show current dimension as 1. Screen:\n" + screen);

        // Verify the current dimension marker is on dim 1
        String[] lines = screen.split("\n");
        boolean foundMarker = false;
        for (String line : lines) {
            if (line.contains("Dimension 1") && line.contains("◄")) {
                foundMarker = true;
                break;
            }
        }
        assertTrue(foundMarker, "Dimension 1 should have the ◄ marker");
    }

    /// Tests that braille characters render without corruption.
    @Test
    void testBrailleCharactersRender() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(20, 5);

        // Create a simple histogram with known pattern
        int[] counts = new int[renderer.getBins()];
        // Create a step pattern
        for (int i = 0; i < renderer.getBins(); i++) {
            counts[i] = (i < renderer.getBins() / 2) ? 100 : 50;
        }

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("braille.fvec")
            .vectorCount(1000)
            .fileDimensions(10)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(0.0f)
            .globalMax(1.0f)
            .samplesLoaded(1000)
            .targetSamples(1000)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(60, 20);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Verify braille characters are present (they're in Unicode block 0x2800-0x28FF)
        boolean hasBraille = false;
        for (char c : screen.toCharArray()) {
            if (c >= 0x2800 && c <= 0x28FF) {
                hasBraille = true;
                break;
            }
        }
        assertTrue(hasBraille, "Screen should contain braille characters");
    }

    /// Tests cursor positioning after render.
    @Test
    void testCursorPositionAfterRender() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(30, 8);

        int[] counts = new int[renderer.getBins()];
        Arrays.fill(counts, 50);

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("cursor.fvec")
            .vectorCount(100)
            .fileDimensions(5)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(0.0f)
            .globalMax(1.0f)
            .samplesLoaded(100)
            .targetSamples(100)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(60, 25);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();

        // Cursor should be positioned at the end of output
        // The frame starts with CURSOR_HOME which positions to (0,0)
        // then content is written, moving cursor to end of last line
        assertNotNull(snapshot.cursor(), "Cursor position should be tracked");
    }

    /// Tests 24-bit color code generation.
    @Test
    void test24BitColorGeneration() {
        // Test that different dimensions produce different colors
        String color0 = ExploreRenderer.get24BitColor(0);
        String color1 = ExploreRenderer.get24BitColor(1);
        String color2 = ExploreRenderer.get24BitColor(2);

        assertNotEquals(color0, color1, "Dimension 0 and 1 should have different colors");
        assertNotEquals(color1, color2, "Dimension 1 and 2 should have different colors");
        assertNotEquals(color0, color2, "Dimension 0 and 2 should have different colors");

        // Verify format: \u001B[38;2;R;G;Bm
        assertTrue(color0.startsWith("\u001B[38;2;"), "Color should use 24-bit ANSI format");
        assertTrue(color0.endsWith("m"), "Color code should end with 'm'");

        // Parse and verify RGB values are in valid range
        String rgb = color0.substring(7, color0.length() - 1); // Remove prefix and 'm'
        String[] parts = rgb.split(";");
        assertEquals(3, parts.length, "Should have R, G, B components");

        for (String part : parts) {
            int value = Integer.parseInt(part);
            assertTrue(value >= 0 && value <= 255, "RGB value should be 0-255");
        }
    }

    /// Tests empty histogram handling.
    @Test
    void testEmptyHistogram() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(30, 5);

        // Empty histogram
        int[] counts = new int[renderer.getBins()];

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("empty.fvec")
            .vectorCount(0)
            .fileDimensions(1)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(0.0f)
            .globalMax(1.0f)
            .samplesLoaded(0)
            .targetSamples(0)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(60, 20);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Should still render without errors
        assertNotNull(screen, "Screen should render even with empty data");
        assertTrue(screen.contains("VECTOR EXPLORER"), "Should contain title");
        assertTrue(screen.contains("0 vectors"), "Should show 0 vectors");
    }

    /// Tests layout doesn't overflow terminal bounds.
    @Test
    void testLayoutFitsTerminal() throws IOException {
        // Use exact terminal size that matches the plot
        int plotWidth = 40;
        int plotHeight = 10;
        int terminalWidth = plotWidth + 15;  // Extra for labels and margins
        int terminalHeight = plotHeight + 14; // Extra for header, axes, legend, loading status, controls

        ExploreRenderer renderer = new ExploreRenderer(plotWidth, plotHeight);

        int[] counts = new int[renderer.getBins()];
        for (int i = 0; i < renderer.getBins(); i++) {
            counts[i] = (int) (100 * Math.sin(i * Math.PI / renderer.getBins()));
        }

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("fit.fvec")
            .vectorCount(5000)
            .fileDimensions(64)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-0.5f)
            .globalMax(0.5f)
            .samplesLoaded(5000)
            .targetSamples(5000)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(terminalWidth, terminalHeight);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Count actual lines
        String[] lines = screen.split("\n");

        // All content should fit within terminal height
        // (Some lines may be blank at the end, that's OK)
        assertTrue(lines.length <= terminalHeight,
            "Content should fit within terminal height. Got " + lines.length + " lines for " + terminalHeight + " height");

        // No line should exceed terminal width
        for (int i = 0; i < lines.length; i++) {
            // Count visible characters (excluding ANSI escape sequences)
            String visibleText = lines[i].replaceAll("\u001B\\[[^m]*m", "");
            assertTrue(visibleText.length() <= terminalWidth,
                "Line " + i + " exceeds terminal width: " + visibleText.length() + " > " + terminalWidth);
        }
    }

    /// Tests loading status display with progress bar.
    @Test
    void testLoadingStatusDisplay() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(40, 10);

        int[] counts = new int[renderer.getBins()];
        Arrays.fill(counts, 50);

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        // Test partial loading (50%)
        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("loading.fvec")
            .vectorCount(10000)
            .fileDimensions(128)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(5000)
            .targetSamples(10000)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(80, 30);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Verify loading status is displayed
        assertTrue(screen.contains("Samples:"), "Should show 'Samples:' label");
        assertTrue(screen.contains("5,000"), "Should show loaded count");
        assertTrue(screen.contains("10,000"), "Should show target count");
        assertTrue(screen.contains("loading..."), "Should show 'loading...' indicator");
        assertTrue(screen.contains("["), "Should show progress bar start");
        assertTrue(screen.contains("]"), "Should show progress bar end");

        // Test complete loading (100%)
        ExploreRenderer.ExploreState completeState = ExploreRenderer.ExploreState.builder()
            .fileName("complete.fvec")
            .vectorCount(10000)
            .fileDimensions(128)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(10000)
            .targetSamples(10000)
            .build();

        String completeFrame = renderer.renderFrame(completeState);
        rig.feedFrame(completeFrame);
        String completeScreen = rig.snapshot().screen();

        // When complete, should not show "loading..."
        assertTrue(completeScreen.contains("100%"), "Should show 100% when complete");
        assertFalse(completeScreen.contains("loading..."), "Should not show 'loading...' when complete");
    }

    /// Tests +/- sample controls are shown.
    @Test
    void testSampleControlsShown() throws IOException {
        ExploreRenderer renderer = new ExploreRenderer(40, 10);

        int[] counts = new int[renderer.getBins()];
        Arrays.fill(counts, 50);

        Set<Integer> enabledDims = new LinkedHashSet<>();
        enabledDims.add(0);

        Map<Integer, int[]> histogramCounts = new HashMap<>();
        histogramCounts.put(0, counts);

        ExploreRenderer.ExploreState state = ExploreRenderer.ExploreState.builder()
            .fileName("controls.fvec")
            .vectorCount(10000)
            .fileDimensions(128)
            .currentDimension(0)
            .enabledDimensions(enabledDims)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(10000)
            .targetSamples(10000)
            .build();

        String frame = renderer.renderFrame(state);

        TerminalTestRig rig = new TerminalTestRig(80, 30);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Verify +/- controls are shown
        assertTrue(screen.contains("+/-: samples"), "Should show +/- sample controls");
    }

    /// Tests grid mode layout fits within terminal bounds.
    @Test
    void testGridLayoutFitsTerminal() throws IOException {
        // Simulate a terminal of 80 columns x 30 rows
        int terminalWidth = 80;
        int terminalHeight = 30;

        // Calculate grid layout like CMD_analyze_explore does
        // Grid layout formula:
        // - Each column uses: (plotWidth + 6) chars for y-axis + content
        // - Plus (gridColumns - 1) separator chars between columns
        // - Total width = gridColumns * (plotWidth + 6) + (gridColumns - 1)
        int minPlotWidth = 20;
        int minPlotHeight = 10;
        int availableWidth = terminalWidth;
        int availableHeight = terminalHeight - 4; // Header (2) + controls (2)

        // Calculate columns: gridColumns * (MIN + 7) <= availableWidth + 1
        int minCellWidth = minPlotWidth + 7; // 6 for y-axis + 1 for separator
        int gridColumns = Math.max(1, (availableWidth + 1) / minCellWidth);

        // Calculate rows: gridRows * (plotHeight + 3) <= availableHeight
        int minCellHeight = minPlotHeight + 3;
        int gridRows = Math.max(1, availableHeight / minCellHeight);

        // Calculate actual plot dimensions
        int totalSeparators = gridColumns - 1;
        int widthForPlots = availableWidth - totalSeparators;
        int gridPlotWidth = Math.max(minPlotWidth, (widthForPlots / gridColumns) - 6);
        int gridPlotHeight = Math.max(minPlotHeight, (availableHeight / gridRows) - 3);

        // Create histogram data for multiple dimensions
        int gridBins = gridPlotWidth * 2;
        Map<Integer, int[]> histogramCounts = new HashMap<>();
        int totalDims = gridRows * gridColumns;
        for (int dim = 0; dim < totalDims; dim++) {
            int[] counts = new int[gridBins];
            Arrays.fill(counts, 50);
            histogramCounts.put(dim, counts);
        }

        ExploreRenderer renderer = new ExploreRenderer(gridPlotWidth, gridPlotHeight);

        ExploreRenderer.GridState state = ExploreRenderer.GridState.builder()
            .fileName("grid_test.fvec")
            .vectorCount(10000)
            .fileDimensions(128)
            .startDimension(0)
            .gridRows(gridRows)
            .gridColumns(gridColumns)
            .plotWidth(gridPlotWidth)
            .plotHeight(gridPlotHeight)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(10000)
            .targetSamples(10000)
            .build();

        String frame = renderer.renderGridFrame(state);

        // Feed to terminal
        TerminalTestRig rig = new TerminalTestRig(terminalWidth, terminalHeight);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Count actual rendered lines
        String[] screenLines = screen.split("\n", -1);

        // Verify content fits
        assertTrue(screen.contains("GRID:"), "Should contain GRID header");
        assertTrue(screen.contains("dim 0"), "Should show dimension 0");
        assertTrue(screen.contains("scroll dims"), "Should show grid controls");

        // Verify line count doesn't exceed terminal height
        // The screen from JediTerm is the terminal buffer, so check we don't overflow
        assertTrue(screenLines.length <= terminalHeight,
            "Grid should fit within terminal height. Got " + screenLines.length +
            " lines for " + terminalHeight + " height terminal.\nScreen:\n" + screen);
    }

    /// Tests grid layout with 2x2 grid specifically.
    @Test
    void testGrid2x2Layout() throws IOException {
        // Create a 2x2 grid with minimum plot sizes
        int gridColumns = 2;
        int gridRows = 2;
        int plotWidth = 20;
        int plotHeight = 10;

        // Calculate required terminal size
        // Width: gridColumns * (plotWidth + 6 for y-axis) + separators between columns
        // Height: 2 (header) + gridRows * (plotHeight + 3) + 2 (controls)
        int requiredWidth = gridColumns * (plotWidth + 6) + (gridColumns - 1);
        int requiredHeight = 4 + gridRows * (plotHeight + 3);

        // Create histogram data for 4 dimensions
        int gridBins = plotWidth * 2;
        Map<Integer, int[]> histogramCounts = new HashMap<>();
        for (int dim = 0; dim < 4; dim++) {
            int[] counts = new int[gridBins];
            Arrays.fill(counts, 50);
            histogramCounts.put(dim, counts);
        }

        ExploreRenderer renderer = new ExploreRenderer(plotWidth, plotHeight);

        ExploreRenderer.GridState state = ExploreRenderer.GridState.builder()
            .fileName("grid2x2.fvec")
            .vectorCount(1000)
            .fileDimensions(128)
            .startDimension(0)
            .gridRows(gridRows)
            .gridColumns(gridColumns)
            .plotWidth(plotWidth)
            .plotHeight(plotHeight)
            .histogramCounts(histogramCounts)
            .globalMin(-1.0f)
            .globalMax(1.0f)
            .samplesLoaded(1000)
            .targetSamples(1000)
            .build();

        String frame = renderer.renderGridFrame(state);

        // Count frame lines
        String[] frameLineArray = frame.split("\r\n", -1);
        int frameLines = frameLineArray.length;

        // Create terminal with exact required size
        TerminalTestRig rig = new TerminalTestRig(requiredWidth, requiredHeight);
        rig.feedFrame(frame);

        TerminalTestRig.Snapshot snapshot = rig.snapshot();
        String screen = snapshot.screen();

        // Verify all 4 dimensions are visible
        assertTrue(screen.contains("dim 0"), "Should show dim 0");
        assertTrue(screen.contains("dim 1"), "Should show dim 1");
        assertTrue(screen.contains("dim 2"), "Should show dim 2");
        assertTrue(screen.contains("dim 3"), "Should show dim 3");

        // Frame line count should match expected
        assertEquals(requiredHeight, frameLines,
            "Frame should have exactly " + requiredHeight + " lines for 2x2 grid with " +
            plotHeight + " row plots. Got " + frameLines);
    }
}
