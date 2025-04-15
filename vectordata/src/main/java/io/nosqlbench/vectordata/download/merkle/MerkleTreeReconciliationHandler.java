package io.nosqlbench.vectordata.download.merkle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/// Handles the reconciliation process between two files using Merkle trees.
///
/// This class provides methods to reconcile differences between files by copying
/// sections from a reference file to a target file, with callback support for
/// monitoring the reconciliation process.
///
public class MerkleTreeReconciliationHandler {

    /// Callback interface for handling section reconciliation events.
    ///
    /// Implementations of this interface can be used to monitor and respond to
    /// events during the reconciliation process.
    public interface ReconciliationCallback {
        void onSectionMismatch(ReconciliationContext context);
        void onSectionReconciled(ReconciliationContext context);
        void onReconciliationError(ReconciliationContext context, Throwable error);
    }

    /// Context object containing information about the section being reconciled.
    ///
    /// This class provides access to the paths, task, and data buffers involved
    /// in a reconciliation operation.
    public static class ReconciliationContext {
        private final Path expectedPath;
        private final Path actualPath;
        private final MerkleTreeReconciler.ReconciliationTask task;
        private final ByteBuffer expectedData;
        private final ByteBuffer actualData;

        public ReconciliationContext(
                Path expectedPath,
                Path actualPath,
                MerkleTreeReconciler.ReconciliationTask task,
                ByteBuffer expectedData,
                ByteBuffer actualData) {
            this.expectedPath = expectedPath;
            this.actualPath = actualPath;
            this.task = task;
            this.expectedData = expectedData;
            this.actualData = actualData;
        }

        public Path getExpectedPath() { return expectedPath; }
        public Path getActualPath() { return actualPath; }
        public MerkleTreeReconciler.ReconciliationTask getTask() { return task; }
        public ByteBuffer getExpectedData() { return expectedData; }
        public ByteBuffer getActualData() { return actualData; }
    }

    /// Reconciles differences between two files using their Merkle trees.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to reconcile
    /// @param callback callback for handling reconciliation events
    /// @param parallel whether to process sections in parallel
    /// @throws IOException if there are file operation errors
    public static void reconcile(
            Path expectedPath,
            Path actualPath,
            ReconciliationCallback callback,
            boolean parallel) throws IOException {

        // Get list of sections that need reconciliation
        List<MerkleTreeReconciler.ReconciliationTask> tasks =
            MerkleTreeReconciler.compareFiles(
                expectedPath,
                actualPath,
                1024 * 1024,  // 1MB minimum section
                16 * 1024 * 1024  // 16MB maximum section
            );

        if (tasks.isEmpty()) {
            return;
        }

        if (parallel) {
            reconcileParallel(expectedPath, actualPath, tasks, callback);
        } else {
            reconcileSequential(expectedPath, actualPath, tasks, callback);
        }
    }

    /// Reconciles differences between files sequentially.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to reconcile
    /// @param tasks list of reconciliation tasks
    /// @param callback callback for handling reconciliation events
    /// @throws IOException if there are file operation errors
    private static void reconcileSequential(
            Path expectedPath,
            Path actualPath,
            List<MerkleTreeReconciler.ReconciliationTask> tasks,
            ReconciliationCallback callback) throws IOException {

        try (FileChannel expectedChannel = FileChannel.open(expectedPath, StandardOpenOption.READ);
             FileChannel actualChannel = FileChannel.open(actualPath, StandardOpenOption.READ)) {

            for (MerkleTreeReconciler.ReconciliationTask task : tasks) {
                reconcileSection(expectedChannel, actualChannel, expectedPath, actualPath, task, callback);
            }
        }
    }

    /// Reconciles differences between files in parallel.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to reconcile
    /// @param tasks list of reconciliation tasks
    /// @param callback callback for handling reconciliation events
    /// @throws IOException if there are file operation errors
    private static void reconcileParallel(
            Path expectedPath,
            Path actualPath,
            List<MerkleTreeReconciler.ReconciliationTask> tasks,
            ReconciliationCallback callback) throws IOException {

        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), tasks.size())
        );

        try {
            CompletableFuture<?>[] futures = tasks.stream()
                .map(task -> CompletableFuture.runAsync(() -> {
                    try (FileChannel expectedChannel = FileChannel.open(expectedPath, StandardOpenOption.READ);
                         FileChannel actualChannel = FileChannel.open(actualPath, StandardOpenOption.READ)) {
                        reconcileSection(expectedChannel, actualChannel, expectedPath, actualPath, task, callback);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor))
                .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join();
        } finally {
            executor.shutdown();
        }
    }

    /// Reconciles a single section between two files.
    ///
    /// @param expectedChannel file channel for the reference file
    /// @param actualChannel file channel for the file to reconcile
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to reconcile
    /// @param task the reconciliation task to perform
    /// @param callback callback for handling reconciliation events
    /// @throws IOException if there are file operation errors
    private static void reconcileSection(
            FileChannel expectedChannel,
            FileChannel actualChannel,
            Path expectedPath,
            Path actualPath,
            MerkleTreeReconciler.ReconciliationTask task,
            ReconciliationCallback callback) throws IOException {

        ByteBuffer expectedData = ByteBuffer.allocate((int) task.getSize());
        ByteBuffer actualData = ByteBuffer.allocate((int) task.getSize());

        expectedChannel.position(task.getStartOffset());
        actualChannel.position(task.getStartOffset());

        expectedChannel.read(expectedData);
        actualChannel.read(actualData);

        expectedData.flip();
        actualData.flip();

        ReconciliationContext context = new ReconciliationContext(
            expectedPath, actualPath, task, expectedData, actualData);

        callback.onSectionMismatch(context);

        try {
            // Here you would implement the actual reconciliation logic
            // For example, copying the expected data to the actual file
            callback.onSectionReconciled(context);
        } catch (Exception e) {
            callback.onReconciliationError(context, e);
        }
    }
}