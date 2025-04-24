package io.nosqlbench.nbvectors.commands.generate.commands;

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


import io.nosqlbench.nbvectors.util.RandomGenerators;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import picocli.CommandLine;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Command to generate a deterministically shuffled sequence of integers and save as ivec file.
 * Uses high-quality PRNG algorithms from Apache Commons RNG library for superior
 * statistical properties.
 */
@CommandLine.Command(name = "ivec-shuffle",
    description = "Generate a deterministically shuffled sequence of integers and save as ivec file")
public class IvecShuffle implements Callable<Integer> {
  private static final int EXIT_SUCCESS = 0;
  private static final int EXIT_FILE_EXISTS = 1;
  private static final int EXIT_ERROR = 2;

  @CommandLine.Option(names = {"-o","--output"}, description = "The output file", required = true)
  private Path outputPath;
  
  /**
   * Validates the output path before execution.
   */
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;
  
  private void validateOutputPath() {
    if (outputPath == null) {
      throw new CommandLine.ParameterException(spec.commandLine(), 
          "Error: No output path provided");
    }
    
    // Normalize the path to resolve any "." or ".." components
    outputPath = outputPath.normalize();
  }

  @CommandLine.Option(names = {"-i", "--interval"},
      description = "Number of values to generate and shuffle (interval length)",
      required = true)
  private long interval;

  @CommandLine.Option(names = {"-s", "--seed"},
      description = "Random seed for reproducible shuffling (0 for non-deterministic)",
      defaultValue = "0")
  private long seed;

  @CommandLine.Option(names = {"-f", "--force"},
      description = "Force overwrite if output file already exists")
  private boolean force = false;
  
  @CommandLine.Option(names = {"-a", "--algorithm"},
      description = "PRNG algorithm to use (XO_SHI_RO_256_PP, XO_SHI_RO_128_PP, SPLIT_MIX_64, MT, KISS)",
      defaultValue = "XO_SHI_RO_256_PP")
  private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

  public static void main(String[] args) {
    IvecShuffle cmd = new IvecShuffle();
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
    // Validate the output path
    try {
      validateOutputPath();
    } catch (CommandLine.ParameterException e) {
      System.err.println(e.getMessage());
      return EXIT_ERROR;
    }
    
    // Check if file exists and handle force option
    if (Files.exists(outputPath) && !force) {
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

      // Determine effective seed: use provided seed, or generate a new one if seed <= 0
      long effectiveSeed = seed;
      if (seed <= 0) {
          effectiveSeed = System.nanoTime() ^ System.currentTimeMillis();
      }
      RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, effectiveSeed);
      
      // Use the improved Fisher-Yates shuffle implementation with high-quality RNG
      RandomGenerators.shuffle(values, rng);

      // Write to ivec file
      try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(outputPath))) {
        for (Integer value : values) {
          dos.writeInt(1); // Dimension is 1 for each entry (scalar)
          dos.writeInt(value); // Write the integer value
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
