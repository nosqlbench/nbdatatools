package io.nosqlbench.nbvectors.commands.verify_knn;

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


import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

import io.jhdf.HdfFile;
import io.nosqlbench.nbvectors.commands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.Neighborhood;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import io.nosqlbench.nbvectors.commands.verify_knn.options.*;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.*;
import io.nosqlbench.nbvectors.commands.verify_knn.readers.KNNData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/// This app will read data from an HDF5 file in the standard vector KNN answer key format,
/// computing correct neighborhoods and comparing them to the provided ones.
///
/// Internally, values may be promoted from int to long and from float to double as needed.
/// For now, floats are used by default, since that is the precision in current test files.
@CommandLine.Command(name = "verify_knn",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "self-check KNN test data answer-keys",
    description = """
        Reads query vectors from HDF5 data, computes KNN neighborhoods, and
        compares them against the answer-key data given. This is a pure Java
        implementation which requires no other vector processing libraries
        or hardware, so it has two key trade-offs with other methods:
        1. It is not as fast as a GPU or TPU. It is not expected to be.
        2. It is a vastly simpler implementation, which makes it arguably easier
           to rely on as a basic verification tool.
        This utility is meant to be used in concert with other tools which are
        faster, but which may benefit from the assurance of a basic coherence check.
        In essence, if you are not sure your test data is self-correct, then use
        this tool to double check it with some sparse sampling.
        
        The currently supported distance functions and file formats are indicated
        by the available command line options.
        
        The pseudo-standard HDF5 KNN answer-key file format is documented here:
        https://github.com/nosqlbench/nbdatatools/blob/main/nbvectors/src/docs/hdf5_vectors.md
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: all tested neighborhoods were correct",
        "2: at least one tested neighborhood was incorrect"
    })
public class CMD_verify_knn implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(CMD_verify_knn.class);
  //  private final static Logger logger = NBLoggerContext.context().getLogger(NBVectors.class);

  @Option(names = {"-i", "--interval"},
      converter = IntervalParser.class,
      defaultValue = "1",
      description = "The index or closed..open range of indices to test")
  private Interval interval;

  @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
  private boolean helpRequested = false;

  @Option(names = {"-f", "--hdf_file"}, required = true, description = "The HDF5 file to load")
  private Path hdfpath;

  @Option(names = {"-d", "--distance_function"},
      defaultValue = "COSINE",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private DistanceFunction distanceFunction;

  @Option(names = {"-k", "--neighborhood_size"},
      defaultValue = "100",
      description = "The neighborhood size")
  private int K;

  @Option(names = {"-l", "--buffer_limit"},
      defaultValue = "-1",
      description = "The buffer size to retain between sorts by distance, selected automatically "
                    + "when unset as a power of ten such that 10 chunks are needed for processing "
                    + "each query")
  private int buffer_limit;

  @Option(names = {"-s", "--status"},
      defaultValue = "all",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private StatusMode output;

  @Option(names = {"-e", "--error_mode"},
      defaultValue = "fail",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private ErrorMode errorMode;

  @Option(names = {"-p", "--phi"}, defaultValue = "0.001d", description = """
      When comparing values which are not exact, due to floating point rounding
      errors, the distance within which the values are considered effectively
      the same.
      """)
  double phi;

  @Option(names = {"--_diaglevel", "-_d"}, hidden = true, description = """
      Internal diagnostic level, sends content directly to the console.""", defaultValue = "ERROR")
  ConsoleDiagnostics diaglevel;


  /// run a verify_knn command
  /// @param args command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    logger.info("starting main");
    logger.info("instancing command");
    CMD_verify_knn command = new CMD_verify_knn();
    logger.info("instancing commandline");
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    logger.info("executing commandline");
    int exitCode = commandLine.execute(args);
    logger.info("exiting main");
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {

    int errors = 0;
    try (StatusView view = getStatusView(); KNNData knndata = new KNNData(new HdfFile(hdfpath))) {
      view.onStart(interval.count());
      for (long index = interval.min(); index < interval.max(); index++) {

        // This is the query vector from the provided test data
        LongIndexedFloatVector providedTestVector = knndata.readHdf5TestVector(index);
        view.onQueryVector(providedTestVector, index, interval.max());

        // This is the neighborhood from the provided test data, corresponding to the query
        // vector (we are checking this one for errors)
        Neighborhood providedNeighborhood = knndata.neighborhood(providedTestVector.index());

        // This neighborhood is the one we calculate from the test and train vectors.
        Neighborhood expectedNeighborhood = computeNeighborhood(providedTestVector, knndata, view);
        // Compute the ordered intersection view of these relative to each other

        NeighborhoodComparison comparison = new NeighborhoodComparison(
            providedTestVector,
            providedNeighborhood,
            expectedNeighborhood
        );
        view.onNeighborhoodComparison(comparison);
        errors += comparison.isError() ? 1 : 0;
        if (errors > 0 && errorMode == ErrorMode.Fail)
          break;
      }
      view.end();
    }
    return errors > 0 ? 2 : 0;
  }

  private StatusView getStatusView() {
    @SuppressWarnings("resource") StatusViewRouter view = new StatusViewRouter();
    switch (output) {
      case All, Progress:
        view.add(new StatusViewLanterna(Math.min(3, interval.count())));
      default:
    }
    switch (output) {
      case All, Stdout:
        view.add(new StatusViewStdout(view.isEmpty()));
      default:
    }
    return view.isEmpty() ? new StatusViewNoOp() : view;
  }


  private Neighborhood computeNeighborhood(
      LongIndexedFloatVector testVector,
      KNNData data,
      StatusView view
  )
  {
    buffer_limit = buffer_limit > 0 ? buffer_limit : computeBufferLimit(data.trainingVectorCount());
    float[] testVecAry = testVector.vector();
    int totalTrainingVectors = data.trainingVectorCount();

    NeighborIndex[] topKResultBuffer = new NeighborIndex[0];
    for (int chunk = 0; chunk < totalTrainingVectors; chunk += buffer_limit) {
      // do a whole chunk, or a partial if that is all that remains
      int chunkSize = Math.min(chunk + buffer_limit, totalTrainingVectors) - chunk;
      // buffer topK + chunkSize neighbors with distance
      NeighborIndex[] unsortedNeighbors = new NeighborIndex[chunkSize + topKResultBuffer.length];
      // but include previous results at the max, so buffer addressing remains 0+...
      System.arraycopy(topKResultBuffer, 0, unsortedNeighbors, chunkSize, topKResultBuffer.length);
      view.onChunk(chunk, chunkSize, totalTrainingVectors);
      // fill the unordered neighborhood with the next batch of vector ordinals and distances
      for (int i = 0; i < chunkSize; i++) {
        int testVectorOrdinal = chunk + i;
        float[] trainVector = data.train(testVectorOrdinal);
        double distance = distanceFunction.distance(testVecAry, trainVector);
        unsortedNeighbors[i] = new NeighborIndex(testVectorOrdinal, distance);
      }

      // put the neighborhood in order and keep the top K results
      Arrays.sort(unsortedNeighbors, Comparator.comparing(NeighborIndex::distance));
      topKResultBuffer = new NeighborIndex[K];
      System.arraycopy(unsortedNeighbors, 0, topKResultBuffer, 0, topKResultBuffer.length);
    }
    return new Neighborhood(topKResultBuffer);
  }

  private int computeBufferLimit(int totalTrainingVectors) {
    int limit = 10;
    while (limit * 10 < totalTrainingVectors && limit < 100000) {
      limit *= 10;
    }
    return limit;
  }


}
