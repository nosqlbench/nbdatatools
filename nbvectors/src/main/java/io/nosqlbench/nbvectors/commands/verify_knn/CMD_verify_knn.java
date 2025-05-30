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


import io.nosqlbench.nbvectors.commands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.commands.verify_knn.options.ConsoleDiagnostics;
import io.nosqlbench.nbvectors.commands.verify_knn.options.ErrorMode;
import io.nosqlbench.nbvectors.commands.verify_knn.options.Interval;
import io.nosqlbench.nbvectors.commands.verify_knn.options.IntervalParser;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusMode;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusView;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusViewLanterna;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusViewNoOp;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusViewRouter;
import io.nosqlbench.nbvectors.commands.verify_knn.statusview.StatusViewStdout;
import io.nosqlbench.vectordata.ProfileDataView;
import io.nosqlbench.vectordata.TestDataGroup;
import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.internalapi.datasets.FloatVectors;
import io.nosqlbench.vectordata.api.Indexed;
import io.nosqlbench.vectordata.internalapi.datasets.IntVectors;
import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborDistances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

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

  @Option(names = {"-i", "--intervals"},
      converter = IntervalParser.class,
      defaultValue = "1",
      description = "The index or closed..open range of indices to test")
  private Interval interval;

  @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
  private boolean helpRequested = false;


  @Parameters(description = "The HDF5 file(s) to load")
  private List<Path> hdfpaths;

  @Option(names = {"-d", "--distance_function"},
      defaultValue = "COSINE",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private DistanceFunction distanceFunction;

  @Option(names = {"-max_k", "--neighborhood_size"},
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
  private double phi;

  @Option(names = {"--profile", "--config"},
      description = "The config profile to use from the dataset. (default ${DEFAULT-VALUE})",
      defaultValue = "ALL")
  private String dataconfig = "ALL";

  @Option(names = {"--_diaglevel", "-_d"}, hidden = true, description = """
      Internal diagnostic level, sends content directly to the console.""", defaultValue = "ERROR")
  ConsoleDiagnostics diaglevel;


  /// run a verify_knn command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    //    System.setProperty("slf4j.internal.verbosity", "ERROR");
    //    System.setProperty(
    //        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
    //        CustomConfigurationFactory.class.getCanonicalName()
    //    );

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
    try {
      // This level catches any exceptions thrown by the command to be shown outside the
      // scope of any terminal UI or other screen modifications.
      for (Path hdfpath : hdfpaths) {

        try (StatusView view = getStatusView(); TestDataGroup datag = new TestDataGroup(hdfpath)) {
          TestDataView data = datag.getProfile("default");


          List<String> configs = new ArrayList<>();
          //          if (this.dataconfig.equalsIgnoreCase("ALL")) {
          //            configs.addAll(data.getProfiles());
          //          } else {
          //            configs.add(this.dataconfig);
          //          }
          configs.add(this.dataconfig);
          for (String config : configs) {

            logger.info("loaded vector data file: {}", data.toString());

            Optional<NeighborDistances> distances = data.getNeighborDistances();

            if (data.getNeighborDistances().isEmpty()) {
              logger.error("neighbor distances are not available in the provided data, so "
                           + "distance-based verification is not possible.");
              return 1;
            } else {
              logger.info("loaded neighbor distances: {}", distances.get().toString());
            }
            IntVectors indices = (IntVectors) data.getNeighborIndices()
                .orElseThrow(() -> new RuntimeException("""
                    Neighbor indices are not available int he provided data, so distance-based verification is not possible.
                    """));
            FloatVectors baseVectors =
                data.getBaseVectors().orElseThrow(() -> new RuntimeException("""
                    Base vectors are not available in the provided data, so distance-based verification is not possible.
                    """));
            FloatVectors queryVectors =
                data.getQueryVectors().orElseThrow(() -> new RuntimeException("""
                    Query vectors are not available in the provided data, so distance-based verification is not possible.
                    """));

            if (baseVectors instanceof FloatVectors floatVectors) {
              logger.info("loaded base vectors: {}", floatVectors.toString());
            } else {
              throw new RuntimeException("unsupported vector type: " + baseVectors.getClass());
            }

            view.onStart(interval.count());
            for (long index = interval.min(); index < interval.max(); index++) {

              // This is the query vector from the provided test data
              Indexed<float[]> query = queryVectors.getIndexed(index);

              view.onQueryVector(query, index, interval.max());

              // This is the neighborhood from the provided test data, corresponding to the query
              // vector (we are checking this one for errors)
              int[] providedNeighborhood = indices.get(index);

              int[] expectedNeighborhood;
              // This neighborhood is the one we calculate from the test and train vectors.

              // This neighborhood is the one we calculate from the test and train vectors.
              expectedNeighborhood = computeNeighborhood(query, baseVectors, view);
              //
              //        Neighborhood expectedNeighborhood = computeNeighborhood(queryVector, knndata, view);
              // Compute the ordered intersection view of these relative to each other

              NeighborhoodComparison comparison =
                  new NeighborhoodComparison(query, providedNeighborhood, expectedNeighborhood);
              view.onNeighborhoodComparison(comparison);
              errors += comparison.isError() ? 1 : 0;
              if (errors > 0 && errorMode == ErrorMode.Fail)
                break;
            }
            view.end();
          }
        }

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
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


  private int[] computeNeighborhood(
      Indexed<float[]> testVector,
      FloatVectors baseVectors,
      StatusView view
  )
  {
    int count = baseVectors.getCount();
    buffer_limit = buffer_limit > 0 ? buffer_limit : computeBufferLimit(count);
    float[] testVecAry = testVector.value();
    int totalTrainingVectors = count;

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
        float[] trainVector = baseVectors.get(testVectorOrdinal);
        double distance = distanceFunction.distance(testVecAry, trainVector);
        unsortedNeighbors[i] = new NeighborIndex(testVectorOrdinal, distance);
      }

      // put the neighborhood in order and keep the top K results
      Arrays.sort(unsortedNeighbors, Comparator.comparing(NeighborIndex::distance));
      topKResultBuffer = new NeighborIndex[K];
      System.arraycopy(unsortedNeighbors, 0, topKResultBuffer, 0, topKResultBuffer.length);
    }
    int[] neighborhood = new int[topKResultBuffer.length];
    for (int i = 0; i < neighborhood.length; i++) {
      neighborhood[i] = (int) topKResultBuffer[i].index();
    }
    return neighborhood;
  }

  private int computeBufferLimit(int totalTrainingVectors) {
    int limit = 10;
    while (limit * 10 < totalTrainingVectors && limit < 100000) {
      limit *= 10;
    }
    return limit;
  }


}
