package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.PathAggregator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InputFile;

public interface ParquetVisitor {


  public enum Depth {
    NONE,
    CALL,
    ROOTS,
    FILES,
    PAGES,
    GROUPS;

    public boolean isEnabledFor(Depth depth) {
      return this.ordinal() >= depth.ordinal();
    }
  }

  ;

  default Depth getTraversalDepth() {
    return Depth.GROUPS;
  };

  default void beforeAll() {};

  default void beforeRoot(PathAggregator path) {};

  default void beforeInputFile(InputFile inputFile) {};

  default void beforePage(BoundedPageStore pageStore) {};

  default void afterGroup(Group group) {};

  default void afterPage(BoundedPageStore pageStore) {};

  default void afterInputFile(InputFile inputFile) {};

  default void afterRoot(PathAggregator path) {};

  default void afterAll() {};

}
