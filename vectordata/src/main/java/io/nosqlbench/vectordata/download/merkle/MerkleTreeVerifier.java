package io.nosqlbench.vectordata.download.merkle;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/// Utility class for verifying and reconciling files using Merkle trees.
///
/// This class provides methods to verify that files match their expected content
/// by comparing Merkle trees, and to reconcile differences by copying sections
/// from a reference file.
///
public class MerkleTreeVerifier {
    private static final Logger logger = Logger.getLogger(MerkleTreeVerifier.class.getName());
    private static final int MAX_ATTEMPTS = 3;
    private static final long DEFAULT_MIN_SECTION = 1024 * 1024; // 1MB
    private static final long DEFAULT_MAX_SECTION = 16 * 1024 * 1024; // 16MB

    /// Result of a verification attempt.
    ///
    /// This class contains information about the outcome of a verification operation,
    /// including whether it was successful and any errors that occurred.
    public static class VerificationResult {
        private final boolean success;
        private final int attempts;
        private final List<MerkleTreeReconciler.ReconciliationTask> remainingTasks;
        private final Exception lastError;

        public VerificationResult(boolean success, int attempts,
                List<MerkleTreeReconciler.ReconciliationTask> remainingTasks, Exception lastError) {
            this.success = success;
            this.attempts = attempts;
            this.remainingTasks = remainingTasks;
            this.lastError = lastError;
        }

        public boolean isSuccess() { return success; }
        public int getAttempts() { return attempts; }
        public List<MerkleTreeReconciler.ReconciliationTask> getRemainingTasks() { return remainingTasks; }
        public Exception getLastError() { return lastError; }
    }

    /// Verifies and reconciles a file against a reference file until they match or max attempts reached.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to verify and reconcile
    /// @return the result of the verification operation
    /// @throws IOException if there are file operation errors
    public static VerificationResult verifyAndReconcile(Path expectedPath, Path actualPath) throws IOException {
        return verifyAndReconcile(expectedPath, actualPath, DEFAULT_MIN_SECTION, DEFAULT_MAX_SECTION, true);
    }

    /// Verifies and reconciles a file against a reference file with custom parameters.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to verify and reconcile
    /// @param minSection minimum section size in bytes
    /// @param maxSection maximum section size in bytes
    /// @param parallel whether to process sections in parallel
    /// @return the result of the verification operation
    /// @throws IOException if there are file operation errors
    public static VerificationResult verifyAndReconcile(
            Path expectedPath,
            Path actualPath,
            long minSection,
            long maxSection,
            boolean parallel) throws IOException {

        AtomicInteger attemptCount = new AtomicInteger(0);
        Exception lastError = null;
        List<MerkleTreeReconciler.ReconciliationTask> lastTasks = null;

        while (attemptCount.get() < MAX_ATTEMPTS) {
            try {
                // Get list of sections that need reconciliation
                List<MerkleTreeReconciler.ReconciliationTask> tasks =
                    MerkleTreeReconciler.compareFiles(expectedPath, actualPath, minSection, maxSection);

                if (tasks.isEmpty()) {
                    // Files match, verification successful
                    return new VerificationResult(true, attemptCount.get(), null, null);
                }

                lastTasks = tasks;
                logger.info(String.format("Attempt %d: Found %d sections needing reconciliation",
                    attemptCount.incrementAndGet(), tasks.size()));

                // Perform reconciliation
                ReconciliationTracker tracker = new ReconciliationTracker(tasks.size());

                MerkleTreeReconciliationHandler.reconcile(expectedPath, actualPath,
                    new MerkleTreeReconciliationHandler.ReconciliationCallback() {
                        @Override
                        public void onSectionMismatch(
                                MerkleTreeReconciliationHandler.ReconciliationContext context) {
                            logger.fine("Processing section: " + context.getTask());
                        }

                        @Override
                        public void onSectionReconciled(
                                MerkleTreeReconciliationHandler.ReconciliationContext context) {
                            tracker.taskCompleted();
                            logger.fine("Completed section: " + context.getTask());
                        }

                        @Override
                        public void onReconciliationError(
                                MerkleTreeReconciliationHandler.ReconciliationContext context,
                                Throwable error) {
                            tracker.taskFailed(error);
                            logger.warning("Failed to reconcile section: " + context.getTask() +
                                " Error: " + error.getMessage());
                        }
                    },
                    parallel
                );

                if (!tracker.isSuccessful()) {
                    throw new IOException("Reconciliation failed: " + tracker.getFailureReason());
                }

            } catch (Exception e) {
                lastError = e;
                logger.warning("Attempt " + attemptCount.get() + " failed: " + e.getMessage());

                if (attemptCount.get() >= MAX_ATTEMPTS) {
                    break;
                }

                // Short delay before retry
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Verification interrupted", ie);
                }
            }
        }

        return new VerificationResult(false, attemptCount.get(), lastTasks, lastError);
    }

    /// Tracks the progress and status of reconciliation tasks.
    ///
    /// This class maintains counters for completed tasks and tracks any failures
    /// that occur during reconciliation.
    private static class ReconciliationTracker {
        private final AtomicInteger completedTasks = new AtomicInteger(0);
        private final AtomicInteger totalTasks;
        private volatile Throwable failureReason;

        public ReconciliationTracker(int totalTasks) {
            this.totalTasks = new AtomicInteger(totalTasks);
        }

        public void taskCompleted() {
            completedTasks.incrementAndGet();
        }

        public void taskFailed(Throwable reason) {
            if (failureReason == null) {
                failureReason = reason;
            }
        }

        public boolean isSuccessful() {
            return failureReason == null && completedTasks.get() == totalTasks.get();
        }

        public Throwable getFailureReason() {
            return failureReason;
        }
    }
}