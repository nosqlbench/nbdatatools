package io.nosqlbench.command.hdf5.subcommands;

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
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.AttrSetConverter;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.HdfTraverser;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.filters.BaseHdfVisitorFilter;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.injectors.BaseHdfVisitorInjector;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.visitors.HdfCompoundVisitor;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.visitors.HdfPrintVisitor;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.visitors.HdfWriterVisitor;
import io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversalv2.NullTransformer;
import io.nosqlbench.vectordata.spec.attributes.syntax.AttrSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Tag HDF5 vector test data files
@CommandLine.Command(name = "tag",
    description = "read or write hdf attributes",
    subcommands = {CommandLine.HelpCommand.class})
public class TagHdf5 implements Callable<Integer> {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(TagHdf5.class);

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

  /// Create a new TagHdf5 command
  /// 
  /// Default constructor that initializes a new tag subcommand instance.
  public TagHdf5() {
  }

  @Override
  public Integer call() {
    logger.info("Executing tag command on file: {}", hdfSource);
    HdfFile in = new HdfFile(hdfSource);

    hdfTarget = hdfTarget == null ? Path.of("_out.hdf5") : hdfTarget;
    WritableHdfFile out = HdfFile.write(hdfTarget);

    NullTransformer transformer = new NullTransformer();
    HdfTraverser hdfTraverser = new HdfTraverser();
    HdfCompoundVisitor traversers = new HdfCompoundVisitor();
    traversers.add(new HdfPrintVisitor());
    traversers.add(new HdfWriterVisitor(out));
    hdfTraverser.traverse(in, traversers);
    
    logger.info("Tag command completed successfully");
    return 0;
  }
}