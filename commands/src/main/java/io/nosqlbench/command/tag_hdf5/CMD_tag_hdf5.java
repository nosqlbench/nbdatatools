package io.nosqlbench.command.tag_hdf5;

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


import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import io.nosqlbench.command.tag_hdf5.traversal.HdfTraverser;
import io.nosqlbench.command.tag_hdf5.traversal.filters.BaseHdfVisitorFilter;
import io.nosqlbench.command.tag_hdf5.traversal.injectors.BaseHdfVisitorInjector;
import io.nosqlbench.command.tag_hdf5.traversal.visitors.HdfCompoundVisitor;
import io.nosqlbench.command.tag_hdf5.traversal.visitors.HdfPrintVisitor;
import io.nosqlbench.command.tag_hdf5.traversal.visitors.HdfWriterVisitor;
import io.nosqlbench.command.tag_hdf5.traversalv2.NullTransformer;
import io.nosqlbench.vectordata.spec.attributes.syntax.AttrSet;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Tag HDF5 vector test data files
@CommandLine.Command(name = "tag_hdf5", description = "read or write hdf attributes",
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_tag_hdf5 implements Callable<Integer>, BundledCommand {

  private final HdfTraverser hdfTraverser =
      new HdfTraverser(new BaseHdfVisitorFilter(), new BaseHdfVisitorInjector());
  @CommandLine.Option(names = {"-i", "--in", "--hdf_source"},
      required = true,
      description = "The HDF5 file to modify")
  private Path hdfSource;

  @CommandLine.Option(names = {"-o", "--out", "--hdf_target"},
      description = "The HDF5 file to modify")
  private Path hdfTarget;

  @CommandLine.Option(names = {"-s", "--set", "--set-attribute"},
      description = "The HDF5 attribute to set",
      converter = AttrSetConverter.class)
  private AttrSet[] attrs;

  /// run a tag_hdf5 command
  /// @param args command line args
  public static void main(String[] args) {
    CMD_tag_hdf5 command = new CMD_tag_hdf5();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
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
