package io.nosqlbench.nbvectors.commands.build_hdf5;

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
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableNode;
import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.spec.attributes.*;
import io.nosqlbench.nbvectors.spec.SpecDataSource;
import io.nosqlbench.nbvectors.spec.SpecDatasets;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.replaceAll;

/// A writer for KNN data in the HDF5 format
public class KnnDataWriter {
  private final static Logger logger = LogManager.getLogger(KnnDataWriter.class);

  private final Path outTemplate;
  private WritableHdfFile writable;
  private final SpecDataSource loader;
  private final Path tempFile;
  private QueryVectorsAttributes queryVectorsAttributes;
  private BaseVectorAttributes baseVectorAttributes;
  private NeighborDistancesAttributes neighborDistancesAttributes;
  private NeighborIndicesAttributes neighborIndicesAttributes;
  private RootGroupAttributes rootGroupAttributes;

  /// create a new KNN data writer
  /// @param outTemplate
  ///     the path to the file to write to
  /// @param loader
  ///     the loader for the data
  public KnnDataWriter(Path outTemplate, SpecDataSource loader) {
    try {
      Path dir = Path.of(".").toAbsolutePath();
      this.tempFile = Files.createTempFile(dir, "hdf5buffer", "hdf5", PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rwxr-xr-x")
      ));
      this.outTemplate = outTemplate;
      this.writable = HdfFile.write(tempFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.loader = loader;
  }

  /// write the training vector data to a dataset
  /// @param iterator
  ///     an iterator for the training vectors
  public void writeBaseVectors(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    WritableDataset writableDataset =
        this.writable.putDataset(SpecDatasets.base_vectors.name(), ary);
    this.baseVectorAttributes = new BaseVectorAttributes(
        ary[0].length,
        ary.length,
        this.loader.getMetadata().model(),
        this.loader.getMetadata().distance_function()
    );
    this.writeAttributes(writableDataset, SpecDatasets.base_vectors, baseVectorAttributes);
    //
    //    // Record number of records in train dataset as an attribute
    //    this.writable.putAttribute(BaseVectorAttributes.count.name(), ary.length);
    //    // Record vector dimensionality (this will be the same for both train and test) as an attribute
    //    this.writable.putAttribute("dimensions", ary[0].length);
  }

  private <T extends Record> void writeAttributes(WritableNode wnode, SpecDatasets dstype, T attrs)
  {
    if (dstype != null && !dstype.getAttributesType().isAssignableFrom(attrs.getClass())) {
      throw new RuntimeException(
          "unable to assign attributes from " + attrs.getClass().getCanonicalName()
          + " to dataset for " + dstype.name());
    }

    try {
      if (attrs instanceof Record record) {
        RecordComponent[] comps = record.getClass().getRecordComponents();
        for (RecordComponent comp : comps) {
          String fieldname = comp.getName();
          Method accessor = comp.getAccessor();
          Object value = accessor.invoke(record);
          if (value instanceof Enum<?> e) {
            value = e.name();
          }
          if (value instanceof Optional<?> o) {
            if (o.isPresent()) {
              value = o.get();
            } else {
              continue;
            }
          }
          if (value == null) {
            throw new RuntimeException(
                "attribute value for requied attribute " + fieldname + " " + "was null");
          }
          wnode.putAttribute(fieldname, value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /// write the test vector data to a dataset
  /// @param iterator
  ///     an iterator for the test vectors
  public void writeQueryVectors(Iterator<LongIndexedFloatVector> iterator) {
    List<LongIndexedFloatVector> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    float[][] ary = new float[vectors.size()][vectors.getFirst().vector().length];
    for (LongIndexedFloatVector vector : vectors) {
      ary[(int) vector.index()] = vector.vector();
    }
    //    WritableDataset ds = new WritableDatasetImpl(ary,"/train",writable);
    WritableDataset wds = this.writable.putDataset(SpecDatasets.query_vectors.name(), ary);
    this.queryVectorsAttributes =
        new QueryVectorsAttributes(loader.getMetadata().model(), ary.length, ary[0].length);
    writeAttributes(wds, SpecDatasets.query_vectors, queryVectorsAttributes);
  }

  /// write the neighbors data to a dataset
  /// @param iterator
  ///     an iterator for the neighbors
  public void writeNeighborsIntStream(Iterator<int[]> iterator) {
    List<int[]> vectors = new ArrayList<>();
    iterator.forEachRemaining(vectors::add);
    int[][] ary = new int[vectors.size()][vectors.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = vectors.get(i);
    }
    WritableDataset wds = this.writable.putDataset(SpecDatasets.neighbor_indices.name(), ary);
    this.neighborIndicesAttributes = new NeighborIndicesAttributes(ary[0].length, ary.length);
    writeAttributes(wds, SpecDatasets.neighbor_indices, neighborIndicesAttributes);
  }

  /// write the distances data to a dataset
  /// @param iterator
  ///     an iterator for the distances
  public void writeDistancesStream(Iterator<float[]> iterator) {
    List<float[]> distances = new ArrayList<>();
    iterator.forEachRemaining(distances::add);
    float[][] ary = new float[distances.size()][distances.getFirst().length];
    for (int i = 0; i < ary.length; i++) {
      ary[i] = distances.get(i);
    }
    WritableDataset wds = this.writable.putDataset(SpecDatasets.neighbor_distances.name(), ary);
    this.neighborDistancesAttributes = new NeighborDistancesAttributes(ary[0].length, ary.length);
    writeAttributes(wds, SpecDatasets.neighbor_distances, neighborDistancesAttributes);

  }

  /// write the filters data to a dataset
  /// @param nodeIterator
  ///     an iterator for the filters
  public void writeFiltersStream(Iterator<PNode<?>> nodeIterator) {
    List<byte[]> predicateEncodings = new ArrayList<>();
    ByteBuffer workingBuffer = ByteBuffer.allocate(5_000_000);

    int maxlen = 0;
    int minlen = Integer.MAX_VALUE;
    while (nodeIterator.hasNext()) {
      PNode<?> node = nodeIterator.next();
      workingBuffer.clear();
      node.encode(workingBuffer);
      workingBuffer.flip();
      byte[] bytes = new byte[workingBuffer.remaining()];
      workingBuffer.get(bytes);
      predicateEncodings.add(bytes);
      maxlen = Math.max(maxlen, bytes.length);
      minlen = Math.min(minlen, bytes.length);
    }
    byte[][] encoded = new byte[predicateEncodings.size()][maxlen];
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = predicateEncodings.get(i);
    }
    this.writable.putDataset(SpecDatasets.query_filters.name(), encoded);
  }

  /// write the data to the file
  public void writeHdf5() {

    try {
      this.writable = HdfFile.write(tempFile);

      System.err.println("writing base vectors...");
      writeBaseVectors(loader.getBaseVectors());

      System.err.println("writing query vectors...");
      writeQueryVectors(loader.getQueryVectors());

      if (loader.getQueryFilters().isPresent()) {
        System.err.println("writing query filters...");
        writeFiltersStream(loader.getQueryFilters().orElseThrow());
      }

      System.err.println("writing neighbors indices...");
      writeNeighborsIntStream(loader.getNeighborIndices());

      System.err.println("writing neighbor distances...");
      writeDistancesStream(loader.getNeighborDistances());

      System.err.println("writing metadata...");
      this.rootGroupAttributes = new RootGroupAttributes(
          loader.getMetadata().model(),
          loader.getMetadata().url(),
          loader.getMetadata().distance_function(),
          loader.getMetadata().notes(),
          loader.getMetadata().license(),
          loader.getMetadata().vendor()
      );
      this.writeAttributes(this.writable, null, rootGroupAttributes);
      this.writable.close();

      relinkName();
    } catch (Exception e) {
      if (Files.exists(tempFile)) {
        try {
          Files.delete(tempFile);
        } catch (IOException ignored) {
        }
      }
      throw new RuntimeException(e);
    }
  }

  private void relinkName() {
    String filenameTemplate = this.outTemplate.getFileName().toString();

    Pattern scanner = Pattern.compile("\\[[^\\]]+\\]");
    Matcher matcher = scanner.matcher(filenameTemplate);
    String newName = matcher.replaceAll(new Resolver(filenameTemplate));
    if (newName.contains("[") || newName.contains("]")) {
      throw new RuntimeException(
          "unresolved tokens in outfile template '" + filenameTemplate + "'");
    }
    Path path = Path.of(newName);
    Path newPath = this.outTemplate.resolveSibling(path);
    try {
      Files.createDirectories(
          newPath.getParent(),
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"))
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      Files.move(
          tempFile,
          newPath,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class Resolver implements Function<MatchResult, String> {
    private final HashMap<String, String> tokens = new HashMap<>() {{
      putAll(Map.of(
          "dimensions",
          baseVectorAttributes.dimensions() + "",
          "max_k",
          neighborIndicesAttributes.max_k() + "",
          "count",
          neighborIndicesAttributes.count() + "",
          "model",
          baseVectorAttributes.model(),
          "function",
          baseVectorAttributes.distance_function().name(),
          "base_count",
          baseVectorAttributes.count() + "",
          "query_count",
          queryVectorsAttributes.count() + ""
      ));
      putAll(Map.of("d", get("dimensions"), "dims", get("dimensions")));
      putAll(Map.of("k", get("max_k"), "maxk", get("max_k")));
      putAll(Map.of("b", get("count"), "base_vectors", get("count"), "vectors", get("count")));
      putAll(Map.of("m", get("model")));
      putAll(Map.of("f", get("function"), "distance_function", get("function")));
      putAll(Map.of(
          "q",
          get("query_count"),
          "queries",
          get("query_count"),
          "query_vectors",
          get("query_count")
      ));
    }};
    private final String templateForDiagnostics;

    Pattern tokenPattern = Pattern.compile(
        """
            (?<pre>[^}]*)
            \\{ (?<token>.+?) \\}
            (?<post>[^]]*)
            """, Pattern.COMMENTS
    );
    Pattern barePattern = Pattern.compile(
        """
            (?<pre>[^a-zA-Z0-9_]*)
            (?<token>[a-zA-Z0-_]+)
            (?<post>[^]]*)
            """, Pattern.COMMENTS
    );

    public Resolver(String templateForDiagnostics) {
      this.templateForDiagnostics = templateForDiagnostics;
    }

    @Override
    public String apply(MatchResult mr) {
      String section = mr.group();
      section = section.substring(1, section.length() - 1);

      Matcher matcher1 = tokenPattern.matcher(section);
      Matcher matcher2 = barePattern.matcher(section);
      MatchResult inner = matcher1.matches() ? matcher1 : matcher2.matches() ? matcher2 : null;
      if (inner == null) {
        throw new RuntimeException(
            "unresolved token in outfile template '" + templateForDiagnostics + "': for '" + section
            + "'");
      }
      String token = inner.group("token");
      String before = inner.group("pre");
      String after = inner.group("post");

      boolean optional = token.endsWith("*");
      token = optional ? token.substring(0, token.length() - 1) : token;
      String replacement = tokens.get(token);
      if (replacement == null) {
        if (optional) {
          return "";
        } else {
          throw new RuntimeException(
              "WARNING: no replacement for token '" + token + "' in outfile template '"
              + templateForDiagnostics + "'");
        }
      }
      String sanitized = replacement.replaceAll("[^a-zA-Z0-9_-]", "");
      if (!sanitized.equals(replacement)) {
        logger.info(
            "sanitized replacement for token '{}' from '{}' to '{}' in outfile template " + "'{}'",
            token,
            replacement,
            sanitized,
            templateForDiagnostics
        );
      }

      return before + sanitized + after;
    }
  }
}
