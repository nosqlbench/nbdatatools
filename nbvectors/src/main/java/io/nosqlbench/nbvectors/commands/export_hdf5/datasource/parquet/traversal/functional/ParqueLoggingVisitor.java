package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVisitor;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.PathAggregator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InputFile;

public class ParqueLoggingVisitor implements ParquetVisitor {
  private static final Logger logger = LogManager.getLogger(ParqueLoggingVisitor.class);

  private final ParquetVisitor inner;
  private final Level level;

  public ParqueLoggingVisitor(ParquetVisitor inner, Level level) {
    this.inner = inner;
    this.level = level;
    logger.log(level, () -> "starting parquet logging visitor for " + inner + " at level " + level);
  }

  @Override
  public void afterAll() {
    logger.log(level, () -> "--> afterAll()");
    inner.afterAll();
    logger.log(level, () -> " <- afterAll()");

  }

  @Override
  public void afterGroup(Group group) {
    logger.log(level, () -> "--> afterGroup(type=" + group.getType()+")");
    inner.afterGroup(group);
    logger.log(level, () -> " <- afterGroup(type=" + group.getType()+")");

  }

  @Override
  public void afterInputFile(InputFile inputFile) {
//    System.out.println("after input file " + inputFile);
    logger.log(level,() -> "--> afterInputFile(file=" + inputFile.toString() + ")");
    inner.afterInputFile(inputFile);
    logger.log(level,() -> " <- afterInputFile(file=" + inputFile.toString() + ")");
  }

  @Override
  public void afterPage(BoundedPageStore pageStore) {
    logger.log(level,() -> "--> afterPage(store=" + pageStore.toString() + ")");
    inner.afterPage(pageStore);
    logger.log(level,() -> "--> afterPage(store=" + pageStore.toString() + ")");
  }

  @Override
  public void afterRoot(PathAggregator rootTraverser) {
    System.out.println("after root " + rootTraverser.getRootPath().getFileName());

    logger.log(level,() -> "--> afterRoot(traverser=" + rootTraverser.toString() + ")");
    inner.afterRoot(rootTraverser);
    logger.log(level,() -> " <- afterRoot(traverser=" + rootTraverser.toString() + ")");

  }

  @Override
  public void beforeAll() {
    logger.log(level,() -> "--> beforeAll()");
    inner.beforeAll();
    logger.log(level,() -> " <- beforeAll()");
  }

  @Override
  public void beforeInputFile(InputFile inputFile) {
//    System.out.println("before input file " + inputFile);
    logger.log(level,() -> "--> beforeInputFile(file=" + inputFile + ")");
    inner.beforeInputFile(inputFile);
    logger.log(level,() -> " <- beforeInputFile(file=" + inputFile + ")");
  }

  @Override
  public void beforePage(BoundedPageStore pageStore) {
    logger.log(level,() -> "--> beforePage(store=" + pageStore + ")");
    inner.beforePage(pageStore);
    logger.log(level,() -> " <- beforePage(store=" + pageStore + ")");
  }

  @Override
  public void beforeRoot(PathAggregator rootTraverser) {
    System.out.println("before root " + rootTraverser.getRootPath());

    logger.log(level,() -> "--> beforeRoot(path=" + rootTraverser + ")");
    inner.beforeRoot(rootTraverser);
    logger.log(level,() -> " <- beforeRoot(path=" + rootTraverser + ")");
  }

  @Override
  public Depth getTraversalDepth() {
    logger.log(level,() -> "<-> getTraversalDepth(depth=" + inner.getTraversalDepth() + ")");
    return inner.getTraversalDepth();
  }
}
