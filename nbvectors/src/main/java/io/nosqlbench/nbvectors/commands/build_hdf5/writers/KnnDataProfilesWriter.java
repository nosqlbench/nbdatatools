package io.nosqlbench.nbvectors.commands.build_hdf5.writers;

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


import com.google.gson.Gson;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import io.jhdf.api.WritableNode;
import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.ArrayChunkingIterable;
import io.nosqlbench.nbvectors.common.FilePaths;
import io.nosqlbench.nbvectors.common.adapters.DataSourceAdapter;
import io.nosqlbench.nbvectors.common.jhdf.StreamableDataset;
import io.nosqlbench.nbvectors.common.jhdf.StreamableDatasetImpl;
import io.nosqlbench.readers.Sized;
import io.nosqlbench.vectordata.utils.SHARED;
import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.layout.FSource;
import io.nosqlbench.vectordata.layout.FView;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.attributes.BaseVectorAttributes;
import io.nosqlbench.vectordata.spec.attributes.NeighborDistancesAttributes;
import io.nosqlbench.vectordata.spec.attributes.NeighborIndicesAttributes;
import io.nosqlbench.vectordata.spec.attributes.QueryVectorsAttributes;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.nosqlbench.vectordata.discovery.TestDataGroup.PROFILES_ATTR;
import static io.nosqlbench.vectordata.discovery.TestDataGroup.SOURCES_GROUP;
import static io.nosqlbench.vectordata.layout.TestGroupLayout.ATTACHMENTS;

/// A writer for KNN data in the HDF5 format
public class KnnDataProfilesWriter {
  private final static Logger logger = LogManager.getLogger(KnnDataProfilesWriter.class);


  private final String outTemplate;
  private final TestGroupLayout fconfig;
  private WritableHdfFile writable;
  //  private final SpecDataSource loader;
  private final Path tempFile;
  private QueryVectorsAttributes queryVectorsAttributes;
  private BaseVectorAttributes baseVectorAttributes;
  private NeighborDistancesAttributes neighborDistancesAttributes;
  private NeighborIndicesAttributes neighborIndicesAttributes;
  private Set<FSource> uniqueSources;
  private Set<String> uniqueInputPaths;
  private Map<Path, Iterable<?>> activeSources = new LinkedHashMap<>();

  /// create a new KNN data writer
  /// @param outfileTemplate
  ///     the path to the file to write to
  /// @param facetedConfig
  ///     the faceted config to use
  /// @see #writeHdf5()
  public KnnDataProfilesWriter(String outfileTemplate, TestGroupLayout facetedConfig)
  {
    try {
      Path dir = Path.of(".").toAbsolutePath();
      this.tempFile = Files.createTempFile(
          dir,
          ".hdf5buffer",
          ".hdf5",
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"))
      );
      this.outTemplate = outfileTemplate;
      this.writable = HdfFile.write(tempFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.fconfig = facetedConfig;
    //    this.loader = loader;
  }

  private void writeDataset(
      String name,
      WritableGroup sourceGroup,
      Iterable<?> iterable
  )
  {
    Iterator<?> tmpiterator = iterable.iterator();
    if (!tmpiterator.hasNext()) {
      throw new RuntimeException("no data found for name '" + name + "'");
    }
    Object next = tmpiterator.next();
    Class<?> fclass = next.getClass();

    ArrayChunkingIterable aci = new ArrayChunkingIterable(fclass, iterable, 1024 * 1024 * 512);
    StreamableDataset streamer = new StreamableDatasetImpl(aci, name, this.writable);
    if (iterable instanceof Sized sized) {
      streamer.modifyDimensions(new int[]{sized.getSize()});
    } else {
      logger.warn("no dimensions are calculated for this dataset during writing");
      throw new RuntimeException("Dimensions are required to be known at time of writing. "
                                 + "Labeling and dataset inventory need this data.");
    }
    WritableDataset wds = sourceGroup.putDataset(name, streamer);
  }

  /// write the data to the file
  public void writeHdf5() {

    try {
      this.writable = HdfFile.write(tempFile);
      System.out.println("tempfile:" + tempFile);

      this.writable.putAttribute(
          PROFILES_ATTR,
          SHARED.gson.toJson(this.fconfig.profiles().profiles())
      );
      List<String> addFiles = this.fconfig.attachments();
      if (addFiles != null) {
        WritableGroup attached = this.writable.putGroup(ATTACHMENTS);
        for (String addFile : addFiles) {
          if (Files.exists(Path.of(addFile))) {
            String content = Files.readString(Path.of(addFile));
            attached.putDataset(addFile, content);
          } else {
            throw new RuntimeException("add file does not exist:" + addFile);
          }
        }
      }

      //      this.writable.putAttribute("config", this.fconfig.toYaml());
      this.uniqueSources = getUniqueSources();
      this.uniqueInputPaths =
          uniqueSources.stream().map(FSource::inpath).collect(Collectors.toSet());

      //    for (String uniqueOrigin : uniqueInputPaths) {
      //      Iterable<float[]> floats = DataSourceAdapter.adaptBaseVectors(Path.of(uniqueOrigin));
      //    }

      Map<String, FProfiles> profileMap = this.fconfig.profiles().profiles();
      for (String profileName : profileMap.keySet()) {
        System.out.println("working with profile:" + profileName);

        FProfiles profile = profileMap.get(profileName);
        Map<String, FView> profileViews = profile.views();
        WritableGroup sourceGroup = this.writable.putGroup(SOURCES_GROUP);

        for (String viewName : profileViews.keySet()) {
          System.out.println("working with view:" + viewName);
          TestDataKind kind = TestDataKind.fromString(viewName);

          FView fView = profileViews.get(viewName);
          FSource source = fView.source();
          Path in = Path.of(source.inpath());
          if (in.toFile().exists()) {
            Iterable<?> iterable = (Iterable<?>) activeSources.computeIfAbsent(
                in, inp -> {
                  System.out.println("loading source:" + inp);
                  return DataSourceAdapter.adaptAnyType(inp);
                }
            );
          } else {
            throw new RuntimeException("source not found:" + in);
          }
        }
      }
      for (Map.Entry<Path, Iterable<?>> entry : activeSources.entrySet()) {
        System.out.println("writing dataset:" + entry.getKey());
        this.writeDataset(entry.getKey().getFileName().toString(), this.writable, entry.getValue());
      }
      writeRootGroupAttributes();
      this.writable.close();
      relinkFile();

    } catch (Exception e) {
      rmTempFile(tempFile);
      throw new RuntimeException(e);
    }
  }


  @NotNull
  private Set<FSource> getUniqueSources() {
    return this.fconfig.profiles().profiles().values().stream()
        .flatMap(fp -> fp.views().values().stream()).map(FView::source).collect(Collectors.toSet());
  }

  private void relinkFile() {
    TestDataGroup vectorData = new TestDataGroup(tempFile);
    Path filePath = vectorData.tokenize(this.outTemplate.toString()).map(Path::of)
        .orElseThrow(() -> new RuntimeException("error tokenizing file"));
    Path newPath = FilePaths.relinkPath(tempFile, filePath);
    logger.debug("moved {} to {}", tempFile, newPath);

  }

  private void rmTempFile(Path tempFile) {
    if (Files.exists(tempFile)) {
      try {
        Files.delete(tempFile);
      } catch (IOException ignored) {
      }
    }
  }

  private void writeRootGroupAttributes() {
    RootGroupAttributes rga = fconfig.attributes();
    System.err.println("writing metadata...");
    System.out.println("root attributes:\n" + rga);
    this.writeAttributes(this.writable, null, rga);
  }


  private <T extends Record> void writeAttributes(WritableNode wnode, TestDataKind dstype, T attrs)
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

          if (value instanceof Optional<?> o) {
            if (o.isPresent()) {
              value = o.get();
            } else {
              continue;
            }
          }

          if (value instanceof Enum<?> e) {
            value = e.name();
          }
          if (value instanceof Map || value instanceof List || value instanceof Set) {
            value = new Gson().toJson(value);
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

  private int sizeOf(Object row0) {
    if (row0 instanceof int[] ia) {
      return ia.length * Integer.BYTES;
    } else if (row0 instanceof byte[] ba) {
      return ba.length * Byte.BYTES;
    } else if (row0 instanceof short[] sa) {
      return sa.length * Short.BYTES;
    } else if (row0 instanceof long[] la) {
      return la.length * Long.BYTES;
    } else if (row0 instanceof float[] fa) {
      return fa.length * Float.BYTES;
    } else if (row0 instanceof double[] da) {
      return da.length * Double.BYTES;
    } else {
      throw new RuntimeException("Unknown type for sizing:" + row0.getClass());
    }
  }

}
