package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.PageSupplier;
import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConvertingIterable;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.PathAggregator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

/// traverse parquet data
/// @see ParquetVisitor
public class ParquetTraversal {

  private final Iterable<PathAggregator> rootTraversers;

  /// create a new parquet traversal
  /// @param roots the root paths to traverse
  public ParquetTraversal(List<Path> roots) {
    rootTraversers = new ConvertingIterable<Path, PathAggregator>(
        roots,
        r -> new PathAggregator(r, true)
    );
  }

  /// traverse the parquet data, calling the appropriate methods on the visitor.
  /// @param visitor the visitor to call
  public void traverse(ParquetVisitor visitor) {

    if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.CALL)) {
      visitor.beforeAll();

      if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.ROOTS)) {

        for (PathAggregator rootTraverser : rootTraversers) {
          visitor.beforeRoot(rootTraverser);

          if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.FILES)) {

            ConvertingIterable<Path, InputFile> inputFilesIter =
                new ConvertingIterable<>(rootTraverser.getFileList(), LocalInputFile::new);

            for (InputFile inputFile : inputFilesIter) {
              visitor.beforeInputFile(inputFile);

              if (visitor.getTraversalDepth().isEnabledFor(ParquetVisitor.Depth.PAGES)) {
                PageSupplier pageSupplier = new PageSupplier(inputFile);
                BoundedPageStore pageStore;
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
              }
              visitor.afterInputFile(inputFile);
            }
          }
          visitor.afterRoot(rootTraverser);
        }
      }
      visitor.afterAll();
    }
  }

}
