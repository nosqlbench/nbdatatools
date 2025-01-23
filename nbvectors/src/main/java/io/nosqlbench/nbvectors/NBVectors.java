package io.nosqlbench.nbvectors;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import picocli.CommandLine;
import picocli.CommandLine.Option;

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
    int exitCode = new CommandLine(new NBVectors()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    List<NeighborhoodComparison> results = new ArrayList<>();
    try (StatusView view = new StatusView(); KNNData data = new KNNData(new HdfFile(hdfpath))) {
      for (long index = interval.start(); index < interval.end(); index++) {
        IndexedFloatVector providedTestVector = readHdf5TestVector(index, data);
        // This neighborhood is the one provided by the test data file
        // (we are checking this one for errors)
        Neighborhood providedNeighborhood = readHdf5Neighborhood(providedTestVector, data);
        // This neighborhood is the one we calculate from the test and train vectors.
        Neighborhood expectedNeighborhood = computeNeighborhood(providedTestVector, data);
        // Compute the ordered intersection view of these relative to each other
        NeighborhoodComparison result =
            compareResults(providedTestVector, providedNeighborhood, expectedNeighborhood, view);
        results.add(result);
      }
    }
    results.forEach(System.out::println);

    return 0;
  }

  private NeighborhoodComparison compareResults(
      IndexedFloatVector testVector,
      Neighborhood providedNeighborhood,
      Neighborhood expectedNeighborhood,
      StatusView view
  )
  {
    long[] provided = providedNeighborhood.getIndices();
    long[] expected = expectedNeighborhood.getIndices();
    if (provided.length != expected.length) {
      throw new RuntimeException("Provided vectors length does not match expected vectors length.");
    }

    Arrays.sort(provided);
    Arrays.sort(expected);

    int a_index = 0, b_index = 0;
    long provided_element, expected_element;

    long[] common_view = new long[provided.length + expected.length];
    long[] provided_view = new long[provided.length + expected.length];
    long[] expected_view = new long[provided.length + expected.length];

    BitSet providedBits = new BitSet(provided.length);
    BitSet expectedBits = new BitSet(expected.length);
    int position = 0;
    while (a_index < provided.length && b_index < expected.length) {

      provided_element=provided[a_index];
      expected_element=expected[b_index];
      if (provided_element==expected_element) {
        providedBits.set(position);
        expectedBits.set(position);
        common_view[position] = provided_element;
        provided_view[position] = provided_element;
        expected_view[position] = provided_element;
        a_index++;
        b_index++;
      } else if (expected_element<provided_element) {
        common_view[position] = expected_element;
        expected_view[position] = expected_element;
        expectedBits.set(position);
        provided_view[position] = -1;
        b_index++;
      } else { // b_element > a_element
        common_view[position] = provided_element;
        expected_view[position] = -1;
        provided_view[position] = provided_element;
        providedBits.set(position);
        a_index++;
      }
      position++;
    }
    common_view = Arrays.copyOf(common_view, position);
    provided_view = Arrays.copyOf(provided_view, position);
    expected_view = Arrays.copyOf(expected_view, position);

    return new NeighborhoodComparison(
        testVector,
        providedNeighborhood,
        expectedNeighborhood,
        provided_view,
        expected_view,
        providedBits,
        expectedBits
    );
  }

  private Neighborhood computeNeighborhood(IndexedFloatVector testVector, KNNData data) {
    float[] testVecAry = testVector.vector();

    int[] trainVectorDimensions = data.train().getDimensions();
    int totalTrainingVectors = trainVectorDimensions[0];
    trainVectorDimensions[0] = 1;
    long[] trainVectorOffsets = new long[trainVectorDimensions.length];

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
        trainVectorOffsets[0] = testVectorOrdinal;
        Object trainVectorData = data.train().getData(trainVectorOffsets, trainVectorDimensions);
        float[] trainVector = ((float[][]) trainVectorData)[0];

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

  private IndexedFloatVector readHdf5TestVector(long index, KNNData data) {
    int[] testVectorDimensions = data.test().getDimensions();
    long[] testVectorOffsets = new long[testVectorDimensions.length];
    // get 1 test vector row at a time; we simply re-use the dim array
    testVectorDimensions[0] = 1;

    testVectorOffsets[0] = index;
    Object testVectorData = data.test().getData(testVectorOffsets, testVectorDimensions);
    float[] testVector = ((float[][]) testVectorData)[0];
    return new IndexedFloatVector(index, testVector);
  }

  private Neighborhood readHdf5Neighborhood(IndexedFloatVector testVector, KNNData datas)
  {
    Neighborhood answerKey = new Neighborhood();
    Dataset neighbors = datas.neighbors();
    Dataset distances = datas.distances();

    int[] dimensions = neighbors.getDimensions();
    long[] sliceOffset = new long[dimensions.length];
    sliceOffset[0] = testVector.index();
    dimensions[0] = 1;

    Object neighborsData = neighbors.getData(sliceOffset, dimensions);

    // normalize type to double, supporting maximum precision for both cases
    long[] neighborIndices = switch (neighborsData) {
      case long[][] longdata -> longdata[0];
      case int[][] intdata -> {
        long[] ary = new long[intdata[0].length];
        for (int i = 0; i < ary.length; i++) {
          ary[i] = intdata[0][i];
        }
        yield ary;
      }
      default -> throw new IllegalStateException("Unexpected value: " + neighborsData);
    };

    Object distancesData = distances.getData(sliceOffset, dimensions);
    float[] neighborDistances = ((float[][]) distancesData)[0];

    for (int i = 0; i < neighborDistances.length; i++) {
      answerKey.add(new NeighborIndex(neighborIndices[i], neighborDistances[i]));
    }

    return answerKey;
  }

}