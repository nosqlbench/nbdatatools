package io.nosqlbench.nbvectors.commands.convert;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks and displays progress information for vector conversion operations.
 */
public class ConversionProgress {
    private static final Logger logger = LogManager.getLogger(ConversionProgress.class);
    
    private final Instant startTime;
    private final AtomicInteger processedCount;
    private Integer totalCount;
    private Instant lastDisplayTime;
    private boolean hasEstimatedTotal;
    
    /**
     * Creates a new progress tracker.
     *
     * @param totalCount The total number of vectors to process (may be null if unknown)
     * @param initialCount The initial count of processed vectors
     */
    public ConversionProgress(Integer totalCount, int initialCount) {
        this.startTime = Instant.now();
        this.lastDisplayTime = startTime;
        this.processedCount = new AtomicInteger(initialCount);
        this.totalCount = totalCount;
        this.hasEstimatedTotal = (totalCount != null);
    }
    
    /**
     * Updates total estimation based on number of vectors processed so far
     * This helps refine the total count estimate as processing continues.
     * 
     * @param processedCount Number of vectors processed so far by reader
     */
    public void updateEstimationFromCount(int processedCount) {
        // If we already have an explicit total, don't override it
        if (totalCount != null && hasEstimatedTotal) {
            return;
        }
        
        if (processedCount > 100) { // Wait until we have a decent sample size
            // Use a basic approach - we estimate max vectors as a multiple of what we've seen
            // This will update and become more accurate as we process more data
            int estimatedTotal = (int)(processedCount * 1.1); // Always estimate a bit more
            
            // Only update if our current estimate is smaller
            if (totalCount == null || estimatedTotal > totalCount) {
                totalCount = estimatedTotal;
                hasEstimatedTotal = false;
            }
        }
    }
    
    /**
     * Increment the count of processed vectors.
     *
     * @return The new count
     */
    public int incrementProcessed() {
        return processedCount.incrementAndGet();
    }
    
    /**
     * Get the current count of processed vectors.
     *
     * @return The current count
     */
    public int getProcessedCount() {
        return processedCount.get();
    }
    
    /**
     * Display the current progress directly to the console.
     */
    public void displayProgress() {
        int processed = processedCount.get();
        Instant now = Instant.now();
        Duration elapsed = Duration.between(startTime, now);
        
        // Calculate processing rate
        double vectorsPerSecond = processed / (elapsed.toMillis() / 1000.0);
        
        // Clear the current line
        System.out.print("\r");
        
        StringBuilder progress = new StringBuilder();
        progress.append("Converting: ").append(processed);
        
        // Add total count info if available
        if (totalCount != null) {
            progress.append("/").append(totalCount);
            // Mark if this is an estimated total
            if (!hasEstimatedTotal) {
                progress.append(" (est)");
            }
            
            // Add percentage
            double percentage = (double) processed / totalCount * 100;
            progress.append(String.format(" [%.1f%%]", percentage));
            
            // Add estimated time remaining
            if (processed > 0) {
                long remaining = (long) ((totalCount - processed) / vectorsPerSecond);
                progress.append(String.format(", ETA: %02d:%02d:%02d", 
                    remaining / 3600, (remaining % 3600) / 60, remaining % 60));
            }
        } else {
            progress.append(" vectors");
        }
        
        // Add rate information
        progress.append(String.format(" | %.1f vectors/sec", vectorsPerSecond));
        
        // Add elapsed time
        long elapsedSeconds = elapsed.getSeconds();
        progress.append(String.format(" | Time: %02d:%02d:%02d", 
            elapsedSeconds / 3600, (elapsedSeconds % 3600) / 60, elapsedSeconds % 60));
        
        // Display progress bar if total is known
        if (totalCount != null) {
            progress.append("\n[");
            int barWidth = 50;
            int completedWidth = (int) ((processed * barWidth) / (double) totalCount);
            
            for (int i = 0; i < barWidth; i++) {
                if (i < completedWidth) {
                    progress.append("=");
                } else if (i == completedWidth) {
                    progress.append(">");
                } else {
                    progress.append(" ");
                }
            }
            progress.append("]");
        }
        
        // Output progress directly to console
        System.out.print(progress.toString());
        System.out.flush();
        lastDisplayTime = now;
        
        // Also log if verbose
        logger.debug(progress.toString());
    }
    
    /**
     * Display final progress summary directly to the console.
     */
    public void displayFinalProgress() {
        int processed = processedCount.get();
        Duration elapsed = Duration.between(startTime, Instant.now());
        double vectorsPerSecond = processed / (elapsed.toMillis() / 1000.0);
        
        // Clear the current line and move to next line
        System.out.print("\r");
        
        StringBuilder summary = new StringBuilder();
        summary.append("Conversion complete! Processed ").append(processed).append(" vectors");
        
        if (totalCount != null) {
            summary.append(String.format(" (%.1f%%)", (double) processed / totalCount * 100));
        }
        
        long elapsedSeconds = elapsed.getSeconds();
        summary.append(String.format(" in %02d:%02d:%02d", 
            elapsedSeconds / 3600, (elapsedSeconds % 3600) / 60, elapsedSeconds % 60));
        summary.append(String.format(", avg %.1f vectors/sec", vectorsPerSecond));
        
        System.out.println(summary.toString());
        
        // Also log if verbose
        logger.info(summary.toString());
    }
}
