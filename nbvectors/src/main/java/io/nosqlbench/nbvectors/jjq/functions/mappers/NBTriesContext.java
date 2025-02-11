package io.nosqlbench.nbvectors.jjq.functions.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.types.AllFieldStats;
import io.nosqlbench.nbvectors.jjq.types.SingleValueStats;
import io.nosqlbench.nbvectors.jjq.types.SingleFieldStats;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class NBTriesContext implements NBIdMapper, StatefulShutdown {

  private final String filepath;
  private final AllFieldStats stats;

  public NBTriesContext(String filepath) {
    this.filepath = filepath;
    Path path = Path.of(filepath);
    if (Files.exists(path)) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        InputStream is = null;
        is = Files.newInputStream(path);
        this.stats = objectMapper.readValue(is, AllFieldStats.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.stats = new AllFieldStats();
    }
  }

  public synchronized long lookupCount(String fieldName, String fieldValue) {
    return stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue).getCount();
  }

  @Override
  public synchronized long lookupId(String fieldName, String fieldValue) {
    return stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue).getIdx();
  }

  @Override
  public synchronized void addInstance(String fieldName, String value) {
    SingleFieldStats singleFieldStats =
        stats.getStatsForField().computeIfAbsent(fieldName, SingleFieldStats::new);
    int size = singleFieldStats.getStatsForValues().size();
    singleFieldStats.getStatsForValues().computeIfAbsent(value, v -> new SingleValueStats(size, 0))
        .increment();
  }

  @Override
  public synchronized void shutdown() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      if (!this.filepath.toLowerCase().endsWith(".json")) {
        throw new RuntimeException("The output file must end in .json");
      }

      OutputStream outputStream = Files.newOutputStream(
          Path.of(this.filepath),
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.CREATE
      );

      objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, this.stats);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "path:" + this.filepath + "\n"
        + stats.summary();
  }

  public NBTriesContext registerShutdownHook(NBStateContext nbctx) {
    nbctx.registerShutdownHook(this);
    return this;
  }
}
