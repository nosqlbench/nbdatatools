package io.nosqlbench.nbvectors.jjq.contexts;

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


import com.fasterxml.jackson.databind.ObjectMapper;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.apis.StatefulShutdown;
import io.nosqlbench.nbvectors.jjq.types.AllFieldStats;
import io.nosqlbench.nbvectors.jjq.types.SingleValueStats;
import io.nosqlbench.nbvectors.jjq.types.SingleFieldStats;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// a stateful context for mapping string values to long values for specific field names
public class NBTriesContext implements NBIdMapper, StatefulShutdown {

  private final String filepath;
  private final AllFieldStats stats;

  /// create a new tries context
  /// @param filepath the path to the file to load the state from
  public NBTriesContext(String filepath) {
    this.filepath = filepath;
    Path path = Path.of(filepath);
    if (Files.exists(path)) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        InputStream is = null;
        is = Files.newInputStream(path);
        System.err.println("loading path:" + path);
        this.stats = objectMapper.readValue(is, AllFieldStats.class);
        System.err.println("loaded path:" + path);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.stats = new AllFieldStats();
    }
  }

  /// lookup the count for a string value
  /// @param fieldName the name of the field to lookup the count for
  /// @param fieldValue the string value to lookup
  /// @return the count for the string value
  public synchronized long lookupCount(String fieldName, String fieldValue) {
    return stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue).getCount();
  }

  /// lookup the id for a string value
  /// @param fieldName the name of the field to lookup the id for
  /// @param fieldValue the string value to lookup
  /// @return the id for the string value
  @Override
  public synchronized long lookupId(String fieldName, String fieldValue) {
    SingleValueStats stats = getOrCreateFor(fieldName, fieldValue);
    return this.stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue)
        .getIdx();
  }

  /// add an instance of a string value to the context
  /// @param name the name of the field to add the instance to
  /// @param value the string value to add
  /// @return stats for the field name and value
  private SingleValueStats getOrCreateFor(String name, String value) {
    SingleFieldStats singleFieldStats =
        stats.getStatsForField().computeIfAbsent(name, SingleFieldStats::new);
    return singleFieldStats.getStatsForValues().computeIfAbsent(
        value,
        v -> new SingleValueStats(singleFieldStats.getStatsForValues().size(), 0)
    );
  }

  /// {@inheritDoc}
  @Override
  public synchronized void addInstance(String fieldName, String value) {
    getOrCreateFor(fieldName, value).increment();
  }

  /// {@inheritDoc}
  @Override
  public synchronized void shutdown() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      String fname = this.filepath.toLowerCase();
      if (!fname.endsWith(".json") && !fname.endsWith(".jsonl")) {
        throw new RuntimeException("The output file must max in .json");
      }

      System.out.println("saving path:" + this.filepath);

      OutputStream outputStream = Files.newOutputStream(
          Path.of(this.filepath),
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.CREATE
      );

      objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, this.stats);

      System.err.println("saved path:" + this.filepath);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "path:" + this.filepath + "\n" + stats.summary();
  }

  /// register a shutdown hook with the provided context, which should then be
  /// called at shutdown time when all nodes are completely processed
  /// @param nbctx the context to register the shutdown hook with
  /// @return this context
  public NBTriesContext registerShutdownHook(NBStateContext nbctx) {
    nbctx.registerShutdownHook(this);
    return this;
  }
}
