package io.nosqlbench.command.generate.subcommands;

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


import io.nosqlbench.command.common.OutputFileOption;
import io.nosqlbench.command.common.RandomSeedOption;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Command to generate a deterministically shuffled sequence of integers and save as ivec file.
 * Uses high-quality PRNG algorithms from Apache Commons RNG library for superior
 * statistical properties.
 */
@CommandLine.Command(name = "ivec-shuffle",
    description = "Generate a deterministically shuffled sequence of integers and save as ivec file")
public class CMD_generate_ivecShuffle implements Callable<Integer> {
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_FILE_EXISTS = 1;
  private static final int EXIT_ERROR = 2;

  @CommandLine.Mixin
  private OutputFileOption outputFileOption = new OutputFileOption();

  @CommandLine.Mixin
  private RandomSeedOption randomSeedOption = new RandomSeedOption();

  @CommandLine.Option(names = {"-i", "--interval"},
      description = "Number of values to generate and shuffle (interval length)",
      required = true)
  private long interval;

  @CommandLine.Option(names = {"-a", "--algorithm"},
      description = "PRNG algorithm to use (XO_SHI_RO_256_PP, XO_SHI_RO_128_PP, SPLIT_MIX_64, MT, KISS)",
      defaultValue = "XO_SHI_RO_256_PP")
  private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  public static void main(String[] args) {
    CMD_generate_ivecShuffle cmd = new CMD_generate_ivecShuffle();
    int exitCode = new CommandLine(cmd).execute(args);
    System.exit(exitCode);
  }

  /**
   * Generates a deterministically shuffled sequence of integers and saves it to an ivec file.
   * Uses high-quality PRNG algorithms from Apache Commons RNG for superior statistical properties.
   * 
   * @return exit code (0 for success, 1 for file exists without force, 2 for other errors)
   * @throws Exception if an error occurs during execution
   */
  @Override
  public Integer call() throws Exception {
    Path outputPath = outputFileOption.getNormalizedOutputPath();

    // Check if file exists and handle force option
    if (outputFileOption.outputExistsWithoutForce()) {
      System.err.println("Error: Output file already exists. Use --force to overwrite.");
      return EXIT_FILE_EXISTS;
    }

    try {
      // Create parent directories if they don't exist and there is a parent path
      Path parent = outputPath.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      // Generate sequence of integers from 0 to interval-1
      if (interval > Integer.MAX_VALUE) {
        System.err.println("Error: Interval too large (maximum allowed is " + Integer.MAX_VALUE + ")");
        return EXIT_ERROR;
      }

      int capacity = (int) interval;
      List<Integer> values = new ArrayList<>(capacity);
      for (int i = 0; i < interval; i++) {
        values.add(i);
      }

      long effectiveSeed = randomSeedOption.getSeed();
      RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, effectiveSeed);

      // Use the improved Fisher-Yates shuffle implementation with high-quality RNG
      RandomGenerators.shuffle(values, rng);

      // Write to ivec file using VectorFileStore
      Optional<VectorFileStreamStore<int[]>> storeOpt = VectorFileIO.streamOut(FileType.xvec, int[].class, outputPath);
      if (storeOpt.isEmpty()) {
        System.err.println("Error: Could not create vector file store for ivec format");
        return EXIT_ERROR;
      }

      try (VectorFileStreamStore<int[]> store = storeOpt.get()) {
        for (Integer value : values) {
          // Create a single-element array for each value (dimension 1)
          int[] vector = new int[] { value };
          store.write(vector);
        }
      }

      System.out.println("Successfully generated shuffled ivec file: " + outputPath);
      System.out.println("Interval: " + interval + ", Seed: " + effectiveSeed + ", Algorithm: " + algorithm);

      return EXIT_SUCCESS;
    } catch (NullPointerException e) {
      System.err.println("Error: Output path or directory issue - " + e.getMessage());
      System.err.println("Make sure the output path is valid: " + outputPath);
      e.printStackTrace();
      return EXIT_ERROR;
    } catch (IOException e) {
      System.err.println("Error: I/O problem when writing to file - " + e.getMessage());
      e.printStackTrace();
      return EXIT_ERROR;
    } catch (Exception e) {
      System.err.println("Error generating shuffled ivec file: " + e.getMessage());
      e.printStackTrace();
      return EXIT_ERROR;
    }
  }
}