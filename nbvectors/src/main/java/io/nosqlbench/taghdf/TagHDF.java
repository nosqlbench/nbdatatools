package io.nosqlbench.taghdf;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.*;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "taghdf", description = "read or write hdf attributes")
public class TagHDF implements Callable<Integer> {

  @CommandLine.Option(names = {"-i", "--in", "--hdf_source"},
      required = true,
      description = "The HDF5 file to modify")
  private Path hdfSource;

  @CommandLine.Option(names = {"-o", "--out", "--hdf_target"},
      required = false,
      description = "The HDF5 file to modify")
  private Path hdfTarget;


  public static void main(String[] args) {
    TagHDF command = new TagHDF();
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
    HdfCompoundTraverser traversers = new HdfCompoundTraverser();
    traversers.add(new HDFPrintTraverser());
    traversers.add(new HdfWriterTraverser(out));
    traverse(in,traversers);
    return 0;
  }

  private void traverse(Node node, HdfTraverser traverser) {
    traverser.enterNode(node);

    switch (node) {
      case HdfFile file -> {
        traverser.enterFile(file);
        for (Node fileElement : file.getChildren().values()) {
          traverse(fileElement,traverser);
        }
        traverser.leaveFile(file);
      }
      case Group group -> {
        traverser.enterGroup(group);
        for (Node groupElement : group.getChildren().values()) {
          traverse(node,traverser);
        }
        traverser.leaveGroup(group);
      }
      case Dataset dataset -> {
        traverser.dataset(dataset);
      }
      case CommittedDatatype cdt -> {
        traverser.committedDataType(cdt);
      }
      default -> {
        throw new RuntimeException("Unrecognized node type: " + node);
      }
    }
    
    traverser.leaveNode(node);
  }

}
