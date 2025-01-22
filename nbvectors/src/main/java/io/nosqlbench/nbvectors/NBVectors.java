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
    try (HdfFile hdfFile = new HdfFile(hdfpath)) {
      Dataset queryVectors = hdfFile.getDatasetByPath("/test");
      Dataset trainVectors = hdfFile.getDatasetByPath("/train");
      Dataset neighbors = hdfFile.getDatasetByPath("/neighbors");
      Dataset distances = hdfFile.getDatasetByPath("/distances");

      int[] queryDims = queryVectors.getDimensions();
      System.out.println("test shape:" + Arrays.toString(queryDims));

      long[] querySliceOffset = new long[queryDims.length];
      queryDims[0] = 1;

      int[] trainDims = trainVectors.getDimensions();
      System.out.println("training shape:" + Arrays.toString(trainDims));

      int totalTrainingVectors = trainDims[0];
      trainDims[0] = 1;
      long[] trainAt = new long[trainDims.length];

      for (long index = interval.start(); index < interval.end(); index++) {
        querySliceOffset[0] = index;
        float[][] queryVs = (float[][]) queryVectors.getData(querySliceOffset, queryDims);
        float[] queryV = queryVs[0];

        NeighborIndex[] lastResults = new NeighborIndex[0];
        System.out.println(
            "query index " + index + ": " + totalTrainingVectors + " at " + trainDims[1] + " "
            + "dimensions, " + buffer_limit + " at a time.");
        for (int chunk = 0; chunk < totalTrainingVectors; chunk += buffer_limit) {
          System.out.print(".");
          System.out.flush();

          int current_buffer_size = Math.min(chunk + buffer_limit, totalTrainingVectors) - chunk;

          NeighborIndex[] buffer = new NeighborIndex[current_buffer_size + lastResults.length];
          System.arraycopy(lastResults, 0, buffer, current_buffer_size, lastResults.length);

          for (int i = 0; i < current_buffer_size; i++) {
            trainAt[0] = chunk + i;
            float[][] trainVs = (float[][]) trainVectors.getData(trainAt, trainDims);
            float[] trainV = trainVs[0];
            double distance = distanceFunction.distance(queryV, trainV);
            buffer[i] = new NeighborIndex(chunk + i, distance);
          }
          Arrays.sort(buffer, Comparator.comparing(NeighborIndex::distance));
          lastResults = new NeighborIndex[K];
          System.arraycopy(buffer, 0, lastResults, 0, lastResults.length);
        }

        Neighborhood precomputedNeighbors = deriveNeighbors(index, neighbors, distances);
        Neighborhood calculatedNeighbors = new Neighborhood(lastResults);

        System.out.println(PrintFormat.format("fromhdf5", index, queryV, precomputedNeighbors));
        System.out.println(PrintFormat.format("computed", index, queryV, calculatedNeighbors));

        Set<Long> calculatedIndices =
            calculatedNeighbors.stream().map(NeighborIndex::index).collect(Collectors.toSet());
        Set<Long> precomputedIndices =
            precomputedNeighbors.stream().map(NeighborIndex::index).collect(Collectors.toSet());
        if (calculatedIndices.size() != precomputedIndices.size()) {
          throw new RuntimeException(String.format(
              "the indices have different sizes: " + "calculated=%d, precomputed=%d",
              calculatedIndices.size(),
              precomputedIndices.size()
          ));
        }
        calculatedIndices.removeAll(precomputedIndices);

        if (!calculatedIndices.isEmpty()) {
          throw new RuntimeException("unequal vectors! " + calculatedIndices.size() + " extra "
                                     + "indices which were not found in the "
                                     + "pre-computed answer key: \n"
                                     + calculatedIndices.toString());
        }

      }

    }

    return 0;
  }

  private Neighborhood deriveNeighbors(long index, Dataset neighbors, Dataset distances) {
    Neighborhood answerKey = new Neighborhood();

    int[] dimensions = neighbors.getDimensions();
    long[] sliceOffset = new long[dimensions.length];
    sliceOffset[0] = index;
    dimensions[0] = 1;

    Object data = neighbors.getData(sliceOffset, dimensions);
    long[] neighborIndices = switch (data) {
      case long[][] longdata -> longdata[0];
      case int[][] intdata -> {
        long[] ary = new long[intdata[0].length];
        for (int i = 0; i < ary.length; i++) {
          ary[i] = intdata[0][i];
        }
        yield ary;
      }
      default -> throw new IllegalStateException("Unexpected value: " + data);
    };

    float[][] neighborDistancesAry = (float[][]) distances.getData(sliceOffset, dimensions);
    float[] neighborDistances = neighborDistancesAry[0];

    for (int i = 0; i < neighborDistances.length; i++) {
      answerKey.add(new NeighborIndex(neighborIndices[i], neighborDistances[i]));
    }

    return answerKey;
  }

}