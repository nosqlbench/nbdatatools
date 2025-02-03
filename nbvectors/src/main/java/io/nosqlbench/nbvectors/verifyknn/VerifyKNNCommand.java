package io.nosqlbench.nbvectors.verifyknn;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

import io.jhdf.HdfFile;
import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;
import io.nosqlbench.nbvectors.verifyknn.datatypes.IndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.verifyknn.datatypes.Neighborhood;
import io.nosqlbench.nbvectors.verifyknn.logging.CustomConfigurationFactory;
import io.nosqlbench.nbvectors.verifyknn.options.*;
import io.nosqlbench.nbvectors.verifyknn.statusview.*;
import io.nosqlbench.nbvectors.verifyknn.readers.KNNData;
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
@CommandLine.Command(name = "verifyknn",
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
public class VerifyKNNCommand implements Callable<Integer> {
  private static Logger logger = LogManager.getLogger(VerifyKNNCommand.class);
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

  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    logger.info("starting main");
    logger.info("instancing command");
    VerifyKNNCommand command = new VerifyKNNCommand();
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
      for (long index = interval.start(); index < interval.end(); index++) {

        // This is the query vector from the provided test data
        IndexedFloatVector providedTestVector = knndata.readHdf5TestVector(index);
        view.onQueryVector(providedTestVector, index, interval.end());

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
      IndexedFloatVector testVector,
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
      // but include previous results at the end, so buffer addressing remains 0+...
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