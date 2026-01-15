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

import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.ExploreRenderer;
import io.nosqlbench.vshapes.extract.ExploreRenderer.ExploreState;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.Console;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

/// Interactive vector data explorer with streaming histogram visualization.
///
/// This command provides an interactive terminal-based interface for exploring
/// vector data dimensions. Features include:
///
/// - **Streaming visualization**: Data is loaded in chunks (default: 100 points)
///   and the histogram updates progressively as data is loaded. Screen updates
///   are rate-limited to a configurable interval (default: 1 second) to reduce IO.
/// - **Dimension navigation**: Use arrow keys to step through dimensions.
/// - **Multi-dimension overlay**: Toggle dimensions on/off to compare distributions.
/// - **24-bit color support**: Each dimension gets a unique color from the full
///   RGB color space for maximum visual distinction.
/// - **Auto-sizing**: Plot dimensions are automatically detected from terminal size.
///   Histogram resolution scales with available horizontal space. Resizing the
///   terminal window triggers automatic recomputation of the histogram.
/// - **Grid mode**: View multiple dimensions simultaneously in a grid layout.
///   Grid size is automatically calculated based on terminal size with minimum
///   plot dimensions of 20×10 characters per cell. Use ←/→ to page through dimensions.
///
/// ## Usage Examples
///
/// ```bash
/// # Explore a vector file interactively
/// nbvectors analyze explore vectors.fvec
///
/// # Start at dimension 5
/// nbvectors analyze explore vectors.fvec -d 5
///
/// # Custom plot size
/// nbvectors analyze explore vectors.fvec --width 120 --height 30
///
/// # Faster screen updates (500ms interval)
/// nbvectors analyze explore vectors.fvec --update-interval 500
/// ```
///
/// ## Keyboard Controls
///
/// - **←/→**: Navigate to previous/next dimension (or page in grid mode)
/// - **↑/↓**: Scroll through visible dimensions when overlaying
/// - **+/-**: Increase/decrease sample size (×2 or ÷2)
/// - **Space**: Toggle current dimension on/off for overlay
/// - **m**: Extract model from current dimension (shows --from-model string)
/// - **g**: Toggle grid mode (view multiple dimensions in a grid)
/// - **r**: Reset to single dimension view
/// - **q**: Quit
@CommandLine.Command(
    name = "explore",
    header = "Interactive vector data explorer",
    description = "Explore vector dimensions interactively with streaming histogram visualization. " +
        "Navigate with arrow keys, toggle dimensions for overlay comparison.",
    exitCodeList = {"0: success", "1: error"}
)
public class CMD_analyze_explore implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_explore.class);

    @CommandLine.Parameters(arity = "1", paramLabel = "FILE",
        description = "Vector file to explore")
    private Path file;

    @CommandLine.Option(names = {"-d", "--start-dimension"},
        description = "Starting dimension index (default: 0)")
    private int startDimension = 0;

    @CommandLine.Option(names = {"--width", "-w"},
        description = "Plot width in characters (default: auto-detect from terminal)")
    private Integer width = null;

    @CommandLine.Option(names = {"--height"},
        description = "Plot height in lines (default: auto-detect from terminal)")
    private Integer height = null;

    @CommandLine.Option(names = {"--chunk-size"},
        description = "Number of data points per chunk (default: 100)")
    private int chunkSize = 100;

    @CommandLine.Option(names = {"--max-samples"},
        description = "Maximum samples to load per dimension (default: 10000)")
    private int maxSamples = 10_000;

    @CommandLine.Option(names = {"--update-interval"},
        description = "Minimum interval between screen updates in milliseconds (default: 1000)")
    private long updateIntervalMs = 1000;

    // State
    private int currentDimension;
    private int fileDimensions;
    private int vectorCount;
    private String fileName;
    private Set<Integer> enabledDimensions = new LinkedHashSet<>();
    private Map<Integer, float[]> dimensionData = new HashMap<>();
    private Map<Integer, int[]> histogramCounts = new HashMap<>();
    private volatile boolean running = true;

    // Plot state
    private float globalMin = Float.MAX_VALUE;
    private float globalMax = Float.MIN_VALUE;

    // Loading state
    private volatile int samplesLoaded = 0;
    private volatile int targetSamples;
    private volatile boolean reloadRequested = false;
    private volatile long lastRenderTime = 0;
    private Thread streamThread;

    // Model extraction state
    private volatile String modelString = null;
    private volatile boolean extractingModel = false;

    // Renderer
    private ExploreRenderer renderer;

    // Terminal size state
    private volatile int terminalWidth = 80;
    private volatile int terminalHeight = 24;
    private volatile int currentPlotWidth = 80;
    private volatile int currentPlotHeight = 20;
    private Thread resizeMonitorThread;

    // Grid mode state
    private static final int MIN_GRID_PLOT_WIDTH = 20;
    private static final int MIN_GRID_PLOT_HEIGHT = 10;
    private volatile boolean gridMode = false;
    private volatile int gridColumns = 1;
    private volatile int gridRows = 1;
    private volatile int gridPlotWidth = MIN_GRID_PLOT_WIDTH;
    private volatile int gridPlotHeight = MIN_GRID_PLOT_HEIGHT;
    private volatile int gridStartDimension = 0;

    @Override
    public Integer call() {
        try {
            if (!Files.exists(file)) {
                System.err.println("Error: File not found: " + file);
                return 1;
            }

            fileName = file.getFileName().toString();

            // Open vector file
            String extension = fileName.substring(fileName.lastIndexOf('.') + 1);
            VectorFileExtension vfExt = VectorFileExtension.fromExtension(extension);
            if (vfExt == null) {
                System.err.println("Error: Unsupported file type: " + extension);
                return 1;
            }

            FileType fileType = vfExt.getFileType();
            Class<?> dataType = vfExt.getDataType();

            try (VectorFileArray<?> vectorFile = VectorFileIO.randomAccess(fileType, dataType, file)) {
                vectorCount = vectorFile.getSize();
                if (vectorCount == 0) {
                    System.err.println("Error: File contains no vectors");
                    return 1;
                }

                // Get dimensions from first vector
                Object first = vectorFile.get(0);
                if (first instanceof float[] f) {
                    fileDimensions = f.length;
                } else if (first instanceof int[] i) {
                    fileDimensions = i.length;
                } else {
                    System.err.println("Error: Unsupported vector type");
                    return 1;
                }

                // Validate start dimension
                if (startDimension < 0 || startDimension >= fileDimensions) {
                    System.err.println("Error: Start dimension " + startDimension +
                        " out of range (0-" + (fileDimensions - 1) + ")");
                    return 1;
                }

                currentDimension = startDimension;
                enabledDimensions.add(currentDimension);

                // Initialize target samples
                targetSamples = Math.min(maxSamples, vectorCount);

                // Detect terminal size and calculate plot dimensions
                calculatePlotDimensions();

                // Create renderer with auto-detected or explicit dimensions
                renderer = new ExploreRenderer(currentPlotWidth, currentPlotHeight);

                // Run interactive loop
                runInteractiveLoop(vectorFile);
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error exploring data", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Runs the main interactive loop.
    private void runInteractiveLoop(VectorFileArray<?> vectorFile) throws IOException {
        Console console = System.console();
        if (console == null) {
            System.err.println("Error: No console available. Run from a terminal.");
            return;
        }

        // Set up raw mode for terminal (platform-specific)
        ProcessBuilder pb = new ProcessBuilder("stty", "-echo", "raw");
        pb.inheritIO();
        try {
            pb.start().waitFor();
        } catch (Exception e) {
            System.err.println("Warning: Could not set raw mode. Using basic input.");
        }

        try {
            System.out.print(ExploreRenderer.CURSOR_HIDE);
            System.out.print(ExploreRenderer.CLEAR_SCREEN);
            System.out.print(ExploreRenderer.CURSOR_HOME);

            // Initial load for current dimension
            loadDimensionData(vectorFile, currentDimension);
            renderPlot();

            // Start data streaming thread
            startStreamingThread(vectorFile);

            // Start resize monitoring (only if auto-detecting size)
            if (width == null || height == null) {
                startResizeMonitor(vectorFile);
            }

            // Input loop
            while (running) {
                int ch = System.in.read();
                if (ch == -1) break;

                if (ch == 27) { // Escape sequence
                    int ch2 = System.in.read();
                    if (ch2 == 91) { // CSI
                        int ch3 = System.in.read();
                        switch (ch3) {
                            case 65 -> scrollUp();      // Up arrow
                            case 66 -> scrollDown();    // Down arrow
                            case 67 -> nextDimension(vectorFile); // Right arrow
                            case 68 -> prevDimension(vectorFile); // Left arrow
                        }
                    }
                } else if (ch == ' ') {
                    toggleDimension(vectorFile);
                } else if (ch == 'r' || ch == 'R') {
                    resetView();
                } else if (ch == '+' || ch == '=') { // + or = (for keyboards without shift)
                    increaseSamples(vectorFile);
                } else if (ch == '-' || ch == '_') { // - or _
                    decreaseSamples(vectorFile);
                } else if (ch == 'm' || ch == 'M') {
                    extractModel();
                } else if (ch == 'g' || ch == 'G') {
                    toggleGridMode(vectorFile);
                } else if (ch == 'q' || ch == 'Q' || ch == 3) { // q or Ctrl+C
                    running = false;
                }
            }

        } finally {
            // Stop resize monitoring
            if (resizeMonitorThread != null) {
                resizeMonitorThread.interrupt();
            }

            // Restore terminal
            System.out.print(ExploreRenderer.CURSOR_SHOW);
            System.out.println();

            pb = new ProcessBuilder("stty", "echo", "cooked");
            pb.inheritIO();
            try {
                pb.start().waitFor();
            } catch (Exception ignored) {}
        }
    }

    /// Starts or restarts the data streaming thread.
    private void startStreamingThread(VectorFileArray<?> vectorFile) {
        // Signal any existing thread to stop
        reloadRequested = true;
        if (streamThread != null && streamThread.isAlive()) {
            streamThread.interrupt();
            try {
                streamThread.join(100);
            } catch (InterruptedException ignored) {}
        }

        reloadRequested = false;
        streamThread = new Thread(() -> streamData(vectorFile));
        streamThread.setDaemon(true);
        streamThread.start();
    }

    /// Streams data in chunks for all enabled dimensions.
    private void streamData(VectorFileArray<?> vectorFile) {
        Random random = new Random(42);
        int currentTarget = targetSamples;
        int[] reservoir = new int[Math.min(currentTarget, vectorCount)];

        // Initialize reservoir with first samples
        for (int i = 0; i < reservoir.length && i < vectorCount; i++) {
            reservoir[i] = i;
        }

        // Reservoir sampling for larger datasets
        for (int i = reservoir.length; i < vectorCount; i++) {
            int j = random.nextInt(i + 1);
            if (j < reservoir.length) {
                reservoir[j] = i;
            }
        }

        // Stream data in chunks
        int loaded = 0;
        samplesLoaded = 0;

        while (running && !reloadRequested && loaded < reservoir.length) {
            int chunkEnd = Math.min(loaded + chunkSize, reservoir.length);

            synchronized (this) {
                for (int dim : enabledDimensions) {
                    float[] data = dimensionData.computeIfAbsent(dim, d -> new float[reservoir.length]);

                    // Resize data array if needed
                    if (data.length < reservoir.length) {
                        float[] newData = new float[reservoir.length];
                        System.arraycopy(data, 0, newData, 0, Math.min(data.length, loaded));
                        data = newData;
                        dimensionData.put(dim, data);
                    }

                    for (int i = loaded; i < chunkEnd; i++) {
                        Object vec = vectorFile.get(reservoir[i]);
                        if (vec instanceof float[] fvec) {
                            data[i] = fvec[dim];
                            if (data[i] < globalMin) globalMin = data[i];
                            if (data[i] > globalMax) globalMax = data[i];
                        }
                    }

                    // Update histogram
                    updateHistogram(dim, loaded, chunkEnd);
                }

                samplesLoaded = chunkEnd;
            }

            loaded = chunkEnd;

            // Rate-limit screen updates to configured interval
            long now = System.currentTimeMillis();
            if (now - lastRenderTime >= updateIntervalMs) {
                lastRenderTime = now;
                renderPlot();
            }
        }

        // Always render final state when loading completes
        if (running && !reloadRequested) {
            lastRenderTime = System.currentTimeMillis();
            renderPlot();
        }
    }

    /// Increases the sample size (doubles it), appending to existing data.
    private void increaseSamples(VectorFileArray<?> vectorFile) {
        int newTarget = Math.min(targetSamples * 2, vectorCount);
        if (newTarget > targetSamples) {
            int oldTarget = targetSamples;
            targetSamples = newTarget;
            // Append additional samples rather than reloading from scratch
            extendSamples(vectorFile, oldTarget);
        }
    }

    /// Extends existing sample data by adding more samples without clearing.
    ///
    /// @param vectorFile the vector file to sample from
    /// @param fromCount the number of samples already loaded
    private void extendSamples(VectorFileArray<?> vectorFile, int fromCount) {
        // Signal any existing thread to stop
        reloadRequested = true;
        if (streamThread != null && streamThread.isAlive()) {
            streamThread.interrupt();
            try {
                streamThread.join(100);
            } catch (InterruptedException ignored) {}
        }

        reloadRequested = false;
        streamThread = new Thread(() -> streamAdditionalData(vectorFile, fromCount));
        streamThread.setDaemon(true);
        streamThread.start();
    }

    /// Streams additional data samples, appending to existing arrays.
    private void streamAdditionalData(VectorFileArray<?> vectorFile, int existingCount) {
        Random random = new Random(System.currentTimeMillis()); // Different seed for new samples
        int additionalSamples = targetSamples - existingCount;

        // Generate random indices for additional samples
        int[] additionalIndices = new int[additionalSamples];
        for (int i = 0; i < additionalSamples; i++) {
            additionalIndices[i] = random.nextInt(vectorCount);
        }

        int loaded = 0;

        while (running && !reloadRequested && loaded < additionalSamples) {
            int chunkEnd = Math.min(loaded + chunkSize, additionalSamples);

            synchronized (this) {
                for (int dim : enabledDimensions) {
                    float[] oldData = dimensionData.get(dim);
                    float[] data;

                    // Expand array if needed
                    if (oldData == null) {
                        data = new float[targetSamples];
                        dimensionData.put(dim, data);
                    } else if (oldData.length < targetSamples) {
                        data = new float[targetSamples];
                        System.arraycopy(oldData, 0, data, 0, Math.min(oldData.length, existingCount));
                        dimensionData.put(dim, data);
                    } else {
                        data = oldData;
                    }

                    // Load additional samples
                    for (int i = loaded; i < chunkEnd; i++) {
                        Object vec = vectorFile.get(additionalIndices[i]);
                        if (vec instanceof float[] fvec) {
                            int dataIndex = existingCount + i;
                            data[dataIndex] = fvec[dim];
                            if (data[dataIndex] < globalMin) globalMin = data[dataIndex];
                            if (data[dataIndex] > globalMax) globalMax = data[dataIndex];
                        }
                    }

                    // Update histogram with new data
                    updateHistogram(dim, existingCount + loaded, existingCount + chunkEnd);
                }

                samplesLoaded = existingCount + chunkEnd;
            }

            loaded = chunkEnd;

            // Rate-limit screen updates
            long now = System.currentTimeMillis();
            if (now - lastRenderTime >= updateIntervalMs) {
                lastRenderTime = now;
                renderPlot();
            }
        }

        // Always render final state
        if (running && !reloadRequested) {
            lastRenderTime = System.currentTimeMillis();
            renderPlot();
        }
    }

    /// Decreases the sample size (halves it).
    private void decreaseSamples(VectorFileArray<?> vectorFile) {
        int newTarget = Math.max(targetSamples / 2, 100); // Minimum 100 samples
        if (newTarget < targetSamples) {
            targetSamples = newTarget;
            reloadData(vectorFile);
        }
    }

    /// Reloads data with the current target sample size.
    private void reloadData(VectorFileArray<?> vectorFile) {
        // Clear existing data
        synchronized (this) {
            dimensionData.clear();
            histogramCounts.clear();
            globalMin = Float.MAX_VALUE;
            globalMax = Float.MIN_VALUE;
            samplesLoaded = 0;
        }

        // Restart streaming
        startStreamingThread(vectorFile);
        renderPlot();
    }

    /// Updates histogram counts for a dimension with new data.
    private void updateHistogram(int dim, int fromIndex, int toIndex) {
        float[] data = dimensionData.get(dim);
        if (data == null) return;

        // Use appropriate bin count based on mode
        int bins = gridMode ? ExploreRenderer.getBinsForWidth(gridPlotWidth) : renderer.getBins();
        int[] counts = histogramCounts.computeIfAbsent(dim, d -> new int[bins]);

        // If bins changed (e.g., mode switch), recreate histogram
        if (counts.length != bins) {
            counts = new int[bins];
            histogramCounts.put(dim, counts);
            // Rebuild from all data
            fromIndex = 0;
        }

        // Handle case where bounds changed
        float min = globalMin;
        float max = globalMax;
        if (min == max) {
            max = min + 1;
        }

        float binWidth = (max - min) / bins;

        for (int i = fromIndex; i < toIndex; i++) {
            int bin = Math.min((int) ((data[i] - min) / binWidth), bins - 1);
            if (bin >= 0 && bin < bins) {
                counts[bin]++;
            }
        }
    }

    /// Loads initial data for a dimension.
    private void loadDimensionData(VectorFileArray<?> vectorFile, int dim) {
        Random random = new Random(42);
        int sampleCount = Math.min(maxSamples, vectorCount);
        float[] data = new float[sampleCount];

        // Quick initial sample
        for (int i = 0; i < Math.min(chunkSize, sampleCount); i++) {
            int idx = random.nextInt(vectorCount);
            Object vec = vectorFile.get(idx);
            if (vec instanceof float[] fvec) {
                data[i] = fvec[dim];
                if (data[i] < globalMin) globalMin = data[i];
                if (data[i] > globalMax) globalMax = data[i];
            }
        }

        synchronized (this) {
            dimensionData.put(dim, data);
            // Use appropriate bin count based on mode
            int bins = gridMode ? ExploreRenderer.getBinsForWidth(gridPlotWidth) : renderer.getBins();
            histogramCounts.put(dim, new int[bins]);
            updateHistogram(dim, 0, Math.min(chunkSize, sampleCount));
        }
    }

    /// Renders the current plot using ExploreRenderer.
    private synchronized void renderPlot() {
        String frame;
        if (gridMode) {
            ExploreRenderer.GridState state = ExploreRenderer.GridState.builder()
                .fileName(fileName)
                .vectorCount(vectorCount)
                .fileDimensions(fileDimensions)
                .startDimension(gridStartDimension)
                .gridRows(gridRows)
                .gridColumns(gridColumns)
                .plotWidth(gridPlotWidth)
                .plotHeight(gridPlotHeight)
                .histogramCounts(histogramCounts)
                .globalMin(globalMin)
                .globalMax(globalMax)
                .samplesLoaded(samplesLoaded)
                .targetSamples(targetSamples)
                .build();
            frame = renderer.renderGridFrame(state);
        } else {
            ExploreState state = ExploreState.builder()
                .fileName(fileName)
                .vectorCount(vectorCount)
                .fileDimensions(fileDimensions)
                .currentDimension(currentDimension)
                .enabledDimensions(enabledDimensions)
                .histogramCounts(histogramCounts)
                .globalMin(globalMin)
                .globalMax(globalMax)
                .samplesLoaded(samplesLoaded)
                .targetSamples(targetSamples)
                .modelString(modelString)
                .build();
            frame = renderer.renderFrame(state);
        }
        System.out.print(frame);
        System.out.flush();
    }

    private void nextDimension(VectorFileArray<?> vectorFile) {
        if (gridMode) {
            // In grid mode, scroll by one page of dimensions
            int pageSize = gridRows * gridColumns;
            int newStart = gridStartDimension + pageSize;
            if (newStart < fileDimensions) {
                gridStartDimension = newStart;
                loadGridDimensions(vectorFile);
            }
        } else {
            currentDimension = (currentDimension + 1) % fileDimensions;
            if (enabledDimensions.size() == 1) {
                enabledDimensions.clear();
                enabledDimensions.add(currentDimension);
                // Clear model string when changing dimensions
                modelString = null;
                // Reload data with full streaming to targetSamples
                reloadData(vectorFile);
            } else {
                renderPlot();
            }
        }
    }

    private void prevDimension(VectorFileArray<?> vectorFile) {
        if (gridMode) {
            // In grid mode, scroll by one page of dimensions
            int pageSize = gridRows * gridColumns;
            int newStart = gridStartDimension - pageSize;
            if (newStart >= 0) {
                gridStartDimension = newStart;
                loadGridDimensions(vectorFile);
            }
        } else {
            currentDimension = (currentDimension - 1 + fileDimensions) % fileDimensions;
            if (enabledDimensions.size() == 1) {
                enabledDimensions.clear();
                enabledDimensions.add(currentDimension);
                // Clear model string when changing dimensions
                modelString = null;
                // Reload data with full streaming to targetSamples
                reloadData(vectorFile);
            } else {
                renderPlot();
            }
        }
    }

    /// Toggles grid mode on/off.
    private void toggleGridMode(VectorFileArray<?> vectorFile) {
        gridMode = !gridMode;
        System.out.print(ExploreRenderer.CLEAR_SCREEN);
        System.out.print(ExploreRenderer.CURSOR_HOME);

        if (gridMode) {
            // Entering grid mode - set start dimension and load grid data
            gridStartDimension = 0;
            loadGridDimensions(vectorFile);
        } else {
            // Exiting grid mode - reset to single dimension view
            enabledDimensions.clear();
            enabledDimensions.add(currentDimension);
            modelString = null;
            reloadData(vectorFile);
        }
    }

    /// Loads data for all dimensions visible in the current grid.
    private void loadGridDimensions(VectorFileArray<?> vectorFile) {
        int pageSize = gridRows * gridColumns;
        int endDim = Math.min(gridStartDimension + pageSize, fileDimensions);

        // Set enabled dimensions to the grid range
        enabledDimensions.clear();
        for (int dim = gridStartDimension; dim < endDim; dim++) {
            enabledDimensions.add(dim);
        }

        // Clear and reload
        synchronized (this) {
            dimensionData.clear();
            histogramCounts.clear();
            globalMin = Float.MAX_VALUE;
            globalMax = Float.MIN_VALUE;
            samplesLoaded = 0;
        }

        // Start streaming
        startStreamingThread(vectorFile);
        renderPlot();
    }

    private void toggleDimension(VectorFileArray<?> vectorFile) {
        if (enabledDimensions.contains(currentDimension)) {
            if (enabledDimensions.size() > 1) {
                enabledDimensions.remove(currentDimension);
            }
        } else {
            enabledDimensions.add(currentDimension);
            // Load data for newly enabled dimension
            if (!dimensionData.containsKey(currentDimension)) {
                loadDimensionData(vectorFile, currentDimension);
            }
        }
        renderPlot();
    }

    private void resetView() {
        enabledDimensions.clear();
        enabledDimensions.add(currentDimension);
        modelString = null;
        renderPlot();
    }

    /// Extracts a model from the current dimension's data.
    private void extractModel() {
        if (extractingModel) return; // Already extracting

        float[] data = dimensionData.get(currentDimension);
        if (data == null || samplesLoaded < 100) {
            modelString = "Need at least 100 samples to extract model";
            renderPlot();
            return;
        }

        extractingModel = true;
        modelString = "Extracting model...";
        renderPlot();

        // Run extraction in background thread
        Thread extractThread = new Thread(() -> {
            try {
                // Create a copy of the data with only loaded samples
                float[] samples = new float[samplesLoaded];
                synchronized (this) {
                    System.arraycopy(data, 0, samples, 0, samplesLoaded);
                }

                // Use BestFitSelector to find the best model
                BestFitSelector selector = BestFitSelector.boundedDataSelector();
                ScalarModel model = selector.selectBest(samples);

                // Convert to JSON representation
                VectorSpaceModelConfig.ComponentConfig config =
                    VectorSpaceModelConfig.ComponentConfig.fromComponentModel(model);

                // Format as compact JSON for --from-model usage
                String json = formatModelJson(config);
                modelString = "dim " + currentDimension + ": " + json;

            } catch (Exception e) {
                modelString = "Error: " + e.getMessage();
            } finally {
                extractingModel = false;
                renderPlot();
            }
        });
        extractThread.setDaemon(true);
        extractThread.start();
    }

    /// Formats a ComponentConfig as a compact JSON string using Gson.
    private String formatModelJson(VectorSpaceModelConfig.ComponentConfig config) {
        com.google.gson.Gson gson = new com.google.gson.GsonBuilder().create();
        return gson.toJson(config);
    }

    /// Detects the current terminal size using stty.
    ///
    /// @return int array with [columns, rows], or null if detection fails
    private int[] detectTerminalSize() {
        try {
            ProcessBuilder pb = new ProcessBuilder("stty", "size");
            pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
            Process process = pb.start();
            String output = new String(process.getInputStream().readAllBytes()).trim();
            process.waitFor();

            if (!output.isEmpty()) {
                String[] parts = output.split("\\s+");
                if (parts.length >= 2) {
                    int rows = Integer.parseInt(parts[0]);
                    int cols = Integer.parseInt(parts[1]);
                    return new int[]{cols, rows};
                }
            }
        } catch (Exception e) {
            logger.debug("Could not detect terminal size: {}", e.getMessage());
        }
        return null;
    }

    /// Calculates plot dimensions from terminal size.
    ///
    /// Reserves space for header (4 lines), legend (variable), loading status (2 lines),
    /// model string (2 lines if present), controls (2 lines), and x-axis (2 lines).
    private void calculatePlotDimensions() {
        int[] size = detectTerminalSize();
        if (size != null) {
            terminalWidth = size[0];
            terminalHeight = size[1];
        }

        // Use explicit dimensions if provided, otherwise auto-calculate
        if (width != null) {
            currentPlotWidth = width;
        } else {
            // Reserve 10 chars for y-axis labels and margins
            currentPlotWidth = Math.max(20, terminalWidth - 10);
        }

        if (height != null) {
            currentPlotHeight = height;
        } else {
            // Reserve lines for: header (4), x-axis (2), legend (est. 3), loading (2),
            // model (2), controls (2), plus some margin
            int reservedLines = 4 + 2 + 3 + 2 + 2 + 2 + 2;
            currentPlotHeight = Math.max(5, terminalHeight - reservedLines);
        }

        // Also calculate grid layout
        calculateGridLayout();
    }

    /// Calculates the grid layout based on terminal size.
    ///
    /// Determines how many plots can fit in a grid with minimum dimensions
    /// of MIN_GRID_PLOT_WIDTH × MIN_GRID_PLOT_HEIGHT per plot.
    ///
    /// Grid layout formula:
    /// - Each column uses: (plotWidth + 6) chars for y-axis + content
    /// - Plus (gridColumns - 1) separator chars between columns
    /// - Total width = gridColumns * (plotWidth + 6) + (gridColumns - 1)
    ///
    /// - Each row uses: 1 title + plotHeight + 1 x-axis + 1 labels = plotHeight + 3 lines
    /// - Total height = 2 (header) + gridRows * (plotHeight + 3) + 2 (controls)
    private void calculateGridLayout() {
        // Reserve space for header (2 lines) and controls (2 lines)
        int availableWidth = terminalWidth;
        int availableHeight = terminalHeight - 4; // Header (2) + controls (2)

        // Calculate how many columns can fit
        // Total width = gridColumns * (plotWidth + 6) + (gridColumns - 1) <= availableWidth
        // For minimum plot width: gridColumns * (MIN + 6) + (gridColumns - 1) <= availableWidth
        // gridColumns * (MIN + 7) <= availableWidth + 1
        // gridColumns <= (availableWidth + 1) / (MIN + 7)
        int minCellWidth = MIN_GRID_PLOT_WIDTH + 7; // 6 for y-axis + 1 for separator
        gridColumns = Math.max(1, (availableWidth + 1) / minCellWidth);

        // Calculate how many rows can fit
        // Total height = gridRows * (plotHeight + 3) <= availableHeight
        int minCellHeight = MIN_GRID_PLOT_HEIGHT + 3;
        gridRows = Math.max(1, availableHeight / minCellHeight);

        // Calculate actual plot dimensions to use available space
        // Width: gridColumns * (plotWidth + 6) + (gridColumns - 1) = availableWidth
        // plotWidth = (availableWidth - gridColumns + 1) / gridColumns - 6
        int totalSeparators = gridColumns - 1;
        int widthForPlots = availableWidth - totalSeparators;
        gridPlotWidth = Math.max(MIN_GRID_PLOT_WIDTH, (widthForPlots / gridColumns) - 6);

        // Height: gridRows * (plotHeight + 3) = availableHeight
        // plotHeight = availableHeight / gridRows - 3
        gridPlotHeight = Math.max(MIN_GRID_PLOT_HEIGHT, (availableHeight / gridRows) - 3);
    }

    /// Starts the resize monitoring thread.
    private void startResizeMonitor(VectorFileArray<?> vectorFile) {
        resizeMonitorThread = new Thread(() -> {
            int lastWidth = terminalWidth;
            int lastHeight = terminalHeight;

            while (running) {
                try {
                    Thread.sleep(500); // Check every 500ms

                    int[] size = detectTerminalSize();
                    if (size != null && (size[0] != lastWidth || size[1] != lastHeight)) {
                        lastWidth = size[0];
                        lastHeight = size[1];
                        handleResize(vectorFile);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        resizeMonitorThread.setDaemon(true);
        resizeMonitorThread.start();
    }

    /// Handles terminal resize by recreating the renderer and reloading data.
    private void handleResize(VectorFileArray<?> vectorFile) {
        // Recalculate dimensions (includes grid layout)
        calculatePlotDimensions();

        // Recreate renderer with new dimensions
        renderer = new ExploreRenderer(currentPlotWidth, currentPlotHeight);

        // Clear screen and redraw
        System.out.print(ExploreRenderer.CLEAR_SCREEN);
        System.out.print(ExploreRenderer.CURSOR_HOME);

        // Clear histogram data (bins may have changed) and reload
        synchronized (this) {
            histogramCounts.clear();
        }

        // Restart streaming to rebuild histograms with new bin count
        if (gridMode) {
            loadGridDimensions(vectorFile);
        } else {
            reloadData(vectorFile);
        }
    }

    private void scrollUp() {
        // Scroll through enabled dimensions
        if (enabledDimensions.size() > 1) {
            Integer[] dims = enabledDimensions.toArray(new Integer[0]);
            for (int i = 0; i < dims.length; i++) {
                if (dims[i] == currentDimension) {
                    currentDimension = dims[(i - 1 + dims.length) % dims.length];
                    break;
                }
            }
        }
        renderPlot();
    }

    private void scrollDown() {
        // Scroll through enabled dimensions
        if (enabledDimensions.size() > 1) {
            Integer[] dims = enabledDimensions.toArray(new Integer[0]);
            for (int i = 0; i < dims.length; i++) {
                if (dims[i] == currentDimension) {
                    currentDimension = dims[(i + 1) % dims.length];
                    break;
                }
            }
        }
        renderPlot();
    }
}
