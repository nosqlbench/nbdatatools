package io.nosqlbench.nbvectors;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/// This app will read data from an HDF5 file in the standard vector KNN answer key format,
/// computing correct neighborhoods and comparing them to the provided ones.
///
/// Internally, values may be promoted from int to long and from float to double as needed.
public class NBVectors implements Callable<Integer> {

  @Option(names = {"-i", "--interval"},
      converter = Interval.Converter.class,
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

  @Option(names = {"-k", "-K", "--neighborhood_size"},
      defaultValue = "100",
      description = "The neighborhood size")
  private int K;

  @Option(names = {"-l", "--buffer_limit over K"},
      defaultValue = "50000",
      description = "The buffer size to retain between sorts by distance")
  private int buffer_limit;

  @Option(names = {"-p", "--print"},
      defaultValue = "all",
      description = "The format to use when printing results")
  private PrintFormat format;

  @Option(names = "-phi",
      defaultValue = "0.001d",
      description = "When comparing values which are not exact, due to floating point rounding "
                    + "errors, the distance within which the values are considered effectively "
                    + "the same.")
  double phi;

  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    int exitCode = new CommandLine(new NBVectors()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    List<NeighborhoodComparison> results = new ArrayList<>();
    try (StatusView view = new StatusView(); KNNData knndata = new KNNData(new HdfFile(hdfpath))) {
      for (long index = interval.start(); index < interval.end(); index++) {
        IndexedFloatVector providedTestVector = knndata.readHdf5TestVector(index);
        // This neighborhood is the one provided by the test data file
        // (we are checking this one for errors)
        Neighborhood providedNeighborhood = knndata.neighborhood(providedTestVector.index());
        // This neighborhood is the one we calculate from the test and train vectors.
        Neighborhood expectedNeighborhood = computeNeighborhood(providedTestVector, knndata);
        // Compute the ordered intersection view of these relative to each other
        NeighborhoodComparison result = new NeighborhoodComparison(
            providedTestVector,
            providedNeighborhood,
            expectedNeighborhood
        );
        results.add(result);
      }
    }
    results.forEach(System.out::println);

    return results.stream().anyMatch(NeighborhoodComparison::isError) ? 2 : 0;
  }

  private Neighborhood computeNeighborhood(IndexedFloatVector testVector, KNNData data) {
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


}