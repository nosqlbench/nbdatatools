package io.nosqlbench.command.json.subcommands.jjq;

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


import io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource.JJQSupplier;
import io.nosqlbench.command.json.subcommands.jjq.evaluator.JJQInvoker;
import io.nosqlbench.command.json.subcommands.jjq.outputs.JsonlFileOutput;
import io.nosqlbench.command.json.subcommands.jjq.outputs.NullOutput;
import io.nosqlbench.command.json.subcommands.jjq.outputs.PrettyConsoleOutput;
import net.thisptr.jackson.jq.Output;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "jjq", description = "run jjq commands with extended functions",
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_jjq implements Callable<Integer> {

  @Option(names = {"-i", "--in"}, required = true)
  private Path inFile;

  @Option(names = {"-o", "--out"},
      description = "The output path for processed JSON, 'null' means discard",
      defaultValue = "stdout")
  private String outPath;

  @CommandLine.Parameters(defaultValue = ".", description = "The jq expression to apply")
  //  @Option(names = {"-q", "--jq", "--query"}, required = false, defaultValue = ".")
  private String jq;

  @Option(names = {"-t", "--threads"}, required = false, defaultValue = "0")
  private int threads;

  @Option(names = {"-p", "--parts"}, required = false, defaultValue = "0")
  private int parts;

  @Option(names = {"-d", "--diag"}, required = false, defaultValue = "false", description = "A debugging mode that provides a simpler execution path, more data, and cleaner stack traces.")
  private boolean diagnose;

  @Option(names = {"-m", "--filemode"},
      defaultValue = "checkpoint",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private Filemode filemode;

  @Option(names = {"--skipdone"},
      required = false,
      description = "If this file exists, silently " + "skip invocation of this " + "command")
  private Path skipFile;

  /// run jjq command
  /// @param args command line args
  public static void main(String[] args) {
//    System.setProperty("slf4j.internal.verbosity", "ERROR");
//    System.setProperty(
//        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
//        CustomConfigurationFactory.class.getCanonicalName()
//    );

    CMD_jjq command = new CMD_jjq();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (skipFile != null && Filemode.checkpoint.isSkip(skipFile)) {
      System.err.println("skipping command, since '" + skipFile + "' is already present");
      return 0;
    }

    Output output = null;
    if (outPath == null || outPath.equals("stdout")) {
      output = new PrettyConsoleOutput();
    } else if (outPath.equalsIgnoreCase("null")) {
      output = new NullOutput();
    } else {
      output = new JsonlFileOutput(Path.of(outPath));
    }

    // Determine how many suppliers to use

    //    //    System.out.println("partitioning");
    //    int partitionCount =
    //        threads != 0 ? threads : (int) (Runtime.getRuntime().availableProcessors() * 0.9);
    //    FilePartitions partitions = FilePartition.of(inFile).partition(partitionCount);
    //    System.err.println(partitions);

    Supplier<String> input = JJQSupplier.path(this.inFile);
    //      BufferOutput output = new BufferOutput(5000000);
    //      JJQInvoker invoker = new JJQInvoker(input, expr, output);
    //      invoker.run();
    //      ConvertingIterable<JsonNode, long[]> converter =
    //          new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoLongNeighborIndices);

    try (JJQInvoker invoker = new JJQInvoker(input, this.jq, output)) {
      invoker.run();
    }


    //    try (NBStateContextHolderHack nbContext = new NBStateContextHolderHack(filemode)) {
    //      Scope rootScope = rootScope(nbContext);
    //
    //      Function<String, JsonNode> mapper = new JsonNodeMapper();
    //      LinkedList<Future<?>> futures;
    //
    //      JsonQuery query = JsonQuery.compile(this.jq, Versions.JQ_1_6);
    //
    //      Output output = null;
    //      if (outPath == null) {
    //        System.err.println("no output specified, discarding output");
    //        output = new NullOutput();
    //      } else if (outPath.toLowerCase().equals("stdout")) {
    //        output = new PrettyConsoleOutput();
    //      } else {
    //        output = new JsonlFileOutput(Path.of(outPath));
    //      }
    //
    //      Scope scope = Scope.newChildScope(rootScope);
    //
    //      //    System.out.println("partitioning");
    //      int partitionCount =
    //          threads != 0 ? threads : (int) (Runtime.getRuntime().availableProcessors() * 0.9);
    //      FilePartitions partitions = FilePartition.of(inFile).partition(partitionCount);
    //      System.err.println(partitions);
    //
    //      if (diagnose) {
    //        try {
    //          for (FilePartition partition : partitions) {
    //            try (ConcurrentSupplier<String> lines = partitions.getFirst().asConcurrentSupplier();) {
    //              JqProc f = new JqProc("diagnostic evaluation", scope, lines, mapper, query, output);
    //              f.run();
    //            }
    //          }
    //        } catch (Exception e) {
    //          throw new RuntimeException(e);
    //        }
    //      } else {
    //        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
    //          futures = new LinkedList<>();
    //
    //          int count = 0;
    //          for (FilePartition partition : partitions) {
    //            count++;
    //            ConcurrentSupplier<String> supplier = partition.asConcurrentSupplier();
    //            JqProc f = new JqProc(partition.toString(), scope, supplier, mapper, query, output);
    //            Future<?> future = exec.submit(f);
    //            futures.addLast(future);
    //          }
    //
    //          while (!futures.isEmpty()) {
    //            try {
    //              Future<?> f = futures.removeLast();
    //              Object result = f.get();
    //              if (result != null) {
    //                System.out.println("result:" + result);
    //              }
    //            } catch (Exception e) {
    //              throw new RuntimeException(e);
    //            }
    //          }
    //        }
    //      }
    //
    //      System.out.println("NbState:");
    //      NBJJQ.getState(rootScope).forEach((max_k, v) -> {
    //        System.out.println(" max_k:" + max_k + ", v:" + v);
    //      });

    return 0;

  }
}
