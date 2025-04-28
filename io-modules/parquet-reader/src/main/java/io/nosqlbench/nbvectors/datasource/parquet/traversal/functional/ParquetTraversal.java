package io.nosqlbench.nbvectors.datasource.parquet.traversal.functional;

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

import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConvertingIterable;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PageSupplier;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathAggregator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/// traverse parquet data
/// @see ParquetVisitor
public class ParquetTraversal {

  private static final Logger logger = LogManager.getLogger(ParquetTraversal.class);
  private final Iterable<PathAggregator> rootTraversers;
  private final int concurrency;

  /// create a new parquet traversal
  /// @param roots the root paths to traverse
  /// @param concurrency the number of concurrent file traversals
  public ParquetTraversal(List<Path> roots, int concurrency) {
    this.rootTraversers = new ConvertingIterable<Path, PathAggregator>(
        roots,
        r -> new PathAggregator(r, true)
    );
    this.concurrency = concurrency;
  }

  /// traverse the parquet data, calling the appropriate methods on the visitor.
  /// @param visitor the visitor to call
  public void traverse(ParquetVisitor visitor) {

    if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.CALL)) {
      visitor.beforeAll();

      if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.ROOTS)) {

        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        // Use a ConcurrentLinkedQueue to maintain order
        ConcurrentLinkedQueue<Future<?>> futures = new ConcurrentLinkedQueue<>();
        AtomicInteger submittedCount = new AtomicInteger(0);

        for (PathAggregator rootTraverser : rootTraversers) {
          visitor.beforeRoot(rootTraverser);

          if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.FILES)) {

            ConvertingIterable<Path, InputFile> inputFilesIter =
                new ConvertingIterable<>(rootTraverser.getFileList(), LocalInputFile::new);

            for (InputFile inputFile : inputFilesIter) {
              final int taskOrder = submittedCount.getAndIncrement();
              logger.info("Submitting file traversal task for file: {} with task order: {}", inputFile, taskOrder);
              // Submit each file as a separate task to the executor
              Future<?> future = executorService.submit(() -> {
                try {
                  traverseFile(inputFile, visitor, taskOrder);
                } catch (Exception e) {
                  logger.error("Error traversing file: " + inputFile, e);
                  // You might want to handle errors differently, e.g., rethrow, count, etc.
                }
              });
              futures.add(future); // Add the future to the queue
            }
          }

          // Completion Barrier: Wait for all tasks for this root to complete before proceeding to afterRoot
          try {
            for (int i = 0; i < submittedCount.get(); i++) {
              Future<?> future = futures.poll();
              if (future != null) {
                future.get(); // Wait for the task to complete
                logger.info("Retired file traversal task: {} task order: {}", rootTraverser, i);
              }
            }
          } catch (InterruptedException | ExecutionException e) {
            logger.error("Error waiting for file traversal tasks to complete.", e);
            Thread.currentThread().interrupt();
          }
          submittedCount.set(0);
          futures.clear();

          visitor.afterRoot(rootTraverser);
        }

        // Shutdown the executor
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            logger.warn("Executor did not terminate in time.");
          }
        } catch (InterruptedException e) {
          executorService.shutdownNow();
          Thread.currentThread().interrupt();
          logger.error("Executor termination interrupted.", e);
        }
      }
      visitor.afterAll();
    }
  }

  private void traverseFile(InputFile inputFile, ParquetVisitor visitor, int taskOrder) {
    visitor.beforeInputFile(inputFile);
    logger.debug("Starting traversal of file {} with task order {}", inputFile, taskOrder);

    if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.PAGES)) {
      try (PageSupplier pageSupplier = new PageSupplier(inputFile)) {
        io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore
            pageStore;
        while ((pageStore = pageSupplier.get()) != null) {
          visitor.beforePage(pageStore);

          if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.GROUPS)) {
            Supplier<Group> groupRecordSupplier = pageStore.get();
            Group group;
            while ((group = groupRecordSupplier.get()) != null) {
              visitor.onGroup(group);
            }
          }
          visitor.afterPage(pageStore);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    visitor.afterInputFile(inputFile);
    logger.debug("Finished traversal of file {} with task order {}", inputFile, taskOrder);
  }
}