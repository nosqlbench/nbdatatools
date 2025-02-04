package io.nosqlbench.nbvectors.taghdf;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSet;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSetConverter;
import io.nosqlbench.nbvectors.taghdf.traversal.HdfTraverser;
import io.nosqlbench.nbvectors.taghdf.traversal.filters.BaseHdfVisitorFilter;
import io.nosqlbench.nbvectors.taghdf.traversal.injectors.BaseHdfVisitorInjector;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfCompoundVisitor;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfPrintVisitor;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfWriterVisitor;
import io.nosqlbench.nbvectors.taghdf.traversalv2.NullTransformer;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "taghdf", description = "read or write hdf attributes")
public class CMD_TagHDF5 implements Callable<Integer> {

  private final HdfTraverser hdfTraverser =
      new HdfTraverser(new BaseHdfVisitorFilter(), new BaseHdfVisitorInjector());
  @CommandLine.Option(names = {"-i", "--in", "--hdf_source"},
      required = true,
      description = "The HDF5 file to modify")
  private Path hdfSource;

  @CommandLine.Option(names = {"-o", "--out", "--hdf_target"},
      required = false,
      description = "The HDF5 file to modify")
  private Path hdfTarget;

  @CommandLine.Option(names = {"-s", "--set", "--set-attribute"},
      required = false,
      description = "The HDF5 attribute to set",
      converter = AttrSetConverter.class)
  private AttrSet[] attrs;


  public static void main(String[] args) {
    CMD_TagHDF5 command = new CMD_TagHDF5();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    HdfFile in = new HdfFile(hdfSource);

    hdfTarget = hdfTarget == null ? Path.of("_out.hdf5") : hdfTarget;
    WritableHdfFile out = HdfFile.write(hdfTarget);

    NullTransformer transformer = new NullTransformer();
    HdfTraverser hdfTraverser = new HdfTraverser();
    HdfCompoundVisitor traversers = new HdfCompoundVisitor();
    traversers.add(new HdfPrintVisitor());
    traversers.add(new HdfWriterVisitor(out));
    hdfTraverser.traverse(in, traversers);
//
//    TransformWalker walker = new TransformWalker(out, transformer);
//    WritableGroup result = walker.traverseNode(in);

//
//    HdfVisitorFilter filter = new RemoveAttrsFilter();
//    HdfCompoundVisitor traversers = new HdfCompoundVisitor();
//    traversers.add(new HdfPrintVisitor());
//    traversers.add(new HdfWriterVisitor(out));
//    hdfTraverser.traverse(in, traversers);
    return 0;
  }

}
