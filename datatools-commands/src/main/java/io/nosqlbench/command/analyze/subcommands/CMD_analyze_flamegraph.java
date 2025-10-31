package io.nosqlbench.command.analyze.subcommands;

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

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

/// Terminal-based flame graph viewer for JFR recordings
/// Uses Unicode block glyphs and 256-color ANSI codes for visualization
@CommandLine.Command(name = "flamegraph",
    description = "Display JFR flame graph in terminal using Unicode and 256-color")
public class CMD_analyze_flamegraph implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "JFR recording file (.jfr)")
    private Path jfrFile;

    @CommandLine.Option(names = {"-w", "--width"}, description = "Display width in characters (default: terminal width)")
    private Integer width;

    @CommandLine.Option(names = {"-h", "--height"}, description = "Maximum flame graph height (default: 40)")
    private int maxHeight = 40;

    @CommandLine.Option(names = {"--min-samples"}, description = "Minimum samples to display (default: 10)")
    private int minSamples = 10;

    @CommandLine.Option(names = {"--event"}, description = "JFR event type (default: jdk.ExecutionSample)")
    private String eventType = "jdk.ExecutionSample";

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_ERROR = 1;

    @Override
    public Integer call() throws Exception {
        try {
            // Get terminal width if not specified
            int displayWidth = width != null ? width : getTerminalWidth();

            System.out.println("Reading JFR file: " + jfrFile);
            System.out.println("Event type: " + eventType);
            System.out.println("Display width: " + displayWidth);
            System.out.println();

            // Parse JFR file and build flame graph
            FlameGraphBuilder builder = new FlameGraphBuilder();
            builder.parseJFR(jfrFile, eventType);

            // Render to terminal
            TerminalFlameGraph renderer = new TerminalFlameGraph(displayWidth, maxHeight, minSamples);
            renderer.render(builder.getRoot());

            return EXIT_SUCCESS;
        } catch (IOException e) {
            System.err.println("Error reading JFR file: " + e.getMessage());
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return EXIT_ERROR;
        }
    }

    private int getTerminalWidth() {
        // Try to detect terminal width
        String cols = System.getenv("COLUMNS");
        if (cols != null) {
            try {
                return Integer.parseInt(cols);
            } catch (NumberFormatException e) {
                // Fall through
            }
        }
        return 120; // Default width
    }

    /**
     * Builds a flame graph tree structure from JFR execution samples.
     */
    static class FlameGraphBuilder {
        private final FlameGraphNode root = new FlameGraphNode("root", null);
        private long totalSamples = 0;

        void parseJFR(Path jfrFile, String eventType) throws IOException {
            try (RecordingFile recording = new RecordingFile(jfrFile)) {
                while (recording.hasMoreEvents()) {
                    RecordedEvent event = recording.readEvent();

                    if (eventType.equals(event.getEventType().getName())) {
                        RecordedStackTrace stackTrace = event.getStackTrace();
                        if (stackTrace != null) {
                            processStackTrace(stackTrace);
                            totalSamples++;
                        }
                    }
                }
            }
        }

        private void processStackTrace(RecordedStackTrace stackTrace) {
            List<RecordedFrame> frames = stackTrace.getFrames();
            if (frames.isEmpty()) {
                return;
            }

            // Build tree from bottom to top (reverse order for flame graph)
            FlameGraphNode current = root;
            for (int i = frames.size() - 1; i >= 0; i--) {
                RecordedFrame frame = frames.get(i);
                String frameName = formatFrame(frame);
                current = current.getOrCreateChild(frameName);
                current.incrementSamples();
            }
        }

        private String formatFrame(RecordedFrame frame) {
            String method = frame.getMethod().getName();
            String className = frame.getMethod().getType().getName();

            // Shorten class name to last component
            int lastDot = className.lastIndexOf('.');
            String shortClassName = lastDot >= 0 ? className.substring(lastDot + 1) : className;

            return shortClassName + "." + method;
        }

        FlameGraphNode getRoot() {
            return root;
        }

        long getTotalSamples() {
            return totalSamples;
        }
    }

    /**
     * Represents a node in the flame graph call tree.
     */
    static class FlameGraphNode {
        private final String name;
        private final String fullName;
        private final Map<String, FlameGraphNode> children = new LinkedHashMap<>();
        private long samples = 0;

        FlameGraphNode(String name, String fullName) {
            this.name = name;
            this.fullName = fullName != null ? fullName : name;
        }

        FlameGraphNode getOrCreateChild(String childName) {
            return children.computeIfAbsent(childName, k -> new FlameGraphNode(k, fullName + ";" + k));
        }

        void incrementSamples() {
            samples++;
        }

        String getName() {
            return name;
        }

        String getFullName() {
            return fullName;
        }

        long getSamples() {
            return samples;
        }

        Collection<FlameGraphNode> getChildren() {
            return children.values();
        }

        boolean hasChildren() {
            return !children.isEmpty();
        }
    }

    /**
     * Renders a flame graph to the terminal using Unicode blocks and 256-color ANSI.
     */
    static class TerminalFlameGraph {
        private final int width;
        private final int maxHeight;
        private final int minSamples;

        // Unicode block characters (from empty to full)
        private static final char[] BLOCKS = {' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'};
        private static final char FULL_BLOCK = '█';

        TerminalFlameGraph(int width, int maxHeight, int minSamples) {
            this.width = width;
            this.maxHeight = maxHeight;
            this.minSamples = minSamples;
        }

        void render(FlameGraphNode root) {
            long totalSamples = root.getSamples();

            String separator = "═".repeat(Math.min(width, 200));
            System.out.println(separator);
            System.out.println("Total samples: " + totalSamples);
            System.out.println(separator);
            System.out.println();

            // Build all levels first for proper hierarchical layout
            List<List<LayoutBox>> levels = buildLayout(root, totalSamples);

            // Render each level from bottom to top
            for (List<LayoutBox> level : levels) {
                if (level.isEmpty()) continue;
                renderLevel(level);
            }

            System.out.println();
            System.out.println(separator);
            System.out.println(String.format("Showing %d levels (of max %d), samples ≥ %d",
                levels.size(), maxHeight, minSamples));
            System.out.println("Legend: Width = CPU time (samples), Color = Package/Class");
            System.out.println(separator);
        }

        /**
         * Build hierarchical layout where children are positioned within parent bounds.
         */
        private List<List<LayoutBox>> buildLayout(FlameGraphNode root, long totalSamples) {
            List<List<LayoutBox>> levels = new ArrayList<>();

            // Start with root's children
            Queue<LevelContext> queue = new LinkedList<>();
            for (FlameGraphNode child : root.getChildren()) {
                if (child.getSamples() >= minSamples) {
                    double childPercentage = (double) child.getSamples() / totalSamples;
                    int childWidth = Math.max(1, (int) (childPercentage * width));
                    queue.add(new LevelContext(child, 0, childWidth, 0));
                }
            }

            // BFS to build levels
            while (!queue.isEmpty() && levels.size() < maxHeight) {
                int levelSize = queue.size();
                List<LayoutBox> currentLevel = new ArrayList<>();

                int currentPos = 0;
                for (int i = 0; i < levelSize; i++) {
                    LevelContext ctx = queue.poll();

                    // Add this node to current level
                    currentLevel.add(new LayoutBox(currentPos, ctx.width, ctx.node, ctx.depth));
                    int parentStart = currentPos;
                    int parentWidth = ctx.width;
                    currentPos += ctx.width;

                    // Add children for next level
                    if (ctx.node.hasChildren()) {
                        int childPos = 0;
                        for (FlameGraphNode child : ctx.node.getChildren()) {
                            if (child.getSamples() >= minSamples) {
                                double childRatio = (double) child.getSamples() / ctx.node.getSamples();
                                int childWidth = Math.max(1, (int) (childRatio * parentWidth));

                                if (childPos + childWidth <= parentWidth) {
                                    queue.add(new LevelContext(child, parentStart + childPos, childWidth, ctx.depth + 1));
                                    childPos += childWidth;
                                }
                            }
                        }
                    }
                }

                if (!currentLevel.isEmpty()) {
                    levels.add(currentLevel);
                }
            }

            return levels;
        }

        private void renderLevel(List<LayoutBox> boxes) {
            // Sort by position
            boxes.sort(Comparator.comparingInt(b -> b.start));

            StringBuilder output = new StringBuilder();
            int currentPos = 0;

            for (LayoutBox box : boxes) {
                // Fill gap with bounds checking
                if (box.start > currentPos) {
                    int gapSize = Math.min(box.start - currentPos, width);
                    if (gapSize > 0) {
                        output.append(" ".repeat(gapSize));
                    }
                    currentPos = box.start;
                }

                // Render box with color
                String fgColor = ANSI.colorForPackage(box.node.getName());
                String bgColor = ANSI.bg256(getBackgroundColorIndex(box.node.getName(), box.depth));

                output.append(fgColor).append(bgColor);
                output.append(createBar(box));
                output.append(ANSI.RESET);

                currentPos = box.start + box.width;
            }

            System.out.println(output);
        }

        private String createBar(LayoutBox box) {
            if (box.width <= 0) {
                return "";
            }

            String label = truncateLabel(box.node.getName(), box.width);

            if (label.isEmpty()) {
                return String.valueOf(FULL_BLOCK).repeat(Math.min(box.width, 200));
            }

            // Center label
            int padding = box.width - label.length();
            int leftPad = Math.max(0, padding / 2);
            int rightPad = Math.max(0, padding - leftPad);

            StringBuilder bar = new StringBuilder();
            if (leftPad > 0) {
                bar.append(String.valueOf(FULL_BLOCK).repeat(Math.min(leftPad, 100)));
            }
            bar.append(label);
            if (rightPad > 0) {
                bar.append(String.valueOf(FULL_BLOCK).repeat(Math.min(rightPad, 100)));
            }

            return bar.toString();
        }

        static class LevelContext {
            final FlameGraphNode node;
            final int start;
            final int width;
            final int depth;

            LevelContext(FlameGraphNode node, int start, int width, int depth) {
                this.node = node;
                this.start = start;
                this.width = width;
                this.depth = depth;
            }
        }

        static class LayoutBox {
            final int start;
            final int width;
            final FlameGraphNode node;
            final int depth;

            LayoutBox(int start, int width, FlameGraphNode node, int depth) {
                this.start = start;
                this.width = width;
                this.node = node;
                this.depth = depth;
            }
        }

        private int getBackgroundColorIndex(String name, int depth) {
            // Use darker colors for background, vary by depth
            int baseColor = 232 + (depth % 8); // 232-239 are grayscale
            return baseColor;
        }

        private String truncateLabel(String name, int availableWidth) {
            if (availableWidth < 4) {
                return "";
            }
            if (name.length() <= availableWidth) {
                return name;
            }
            return name.substring(0, availableWidth - 2) + "..";
        }
    }

    /**
     * ANSI color code utilities for 256-color terminal support.
     */
    static class ANSI {
        static final String RESET = "\033[0m";

        /**
         * Generate 256-color foreground color code.
         *
         * @param colorIndex 0-255 color index
         * @return ANSI color code
         */
        static String color256(int colorIndex) {
            return "\033[38;5;" + colorIndex + "m";
        }

        /**
         * Generate 256-color background color code.
         *
         * @param colorIndex 0-255 color index
         * @return ANSI color code
         */
        static String bg256(int colorIndex) {
            return "\033[48;5;" + colorIndex + "m";
        }

        /**
         * Get color based on package/class name for consistent coloring.
         */
        static String colorForPackage(String name) {
            if (name.contains("java.")) return color256(33);  // Yellow
            if (name.contains("jdk.")) return color256(39);   // Cyan
            if (name.contains("sun.")) return color256(94);   // Light magenta
            if (name.contains("nosqlbench")) return color256(82); // Green
            if (name.contains("ExecutionSample")) return color256(196); // Red

            // Default: hash to a color
            int hash = Math.abs(name.hashCode());
            return color256(16 + (hash % 216));
        }
    }

    public static void main(String[] args) {
        CMD_analyze_flamegraph cmd = new CMD_analyze_flamegraph();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }
}
