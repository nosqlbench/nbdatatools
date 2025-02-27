package io.nosqlbench.nbvectors.jjq.contexts;

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

  public synchronized long lookupCount(String fieldName, String fieldValue) {
    return stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue).getCount();
  }

  @Override
  public synchronized long lookupId(String fieldName, String fieldValue) {
    SingleValueStats stats = getOrCreateFor(fieldName, fieldValue);
    return this.stats.getStatsForField().get(fieldName).getStatsForValues().get(fieldValue)
        .getIdx();
  }

  private SingleValueStats getOrCreateFor(String name, String value) {
    SingleFieldStats singleFieldStats =
        stats.getStatsForField().computeIfAbsent(name, SingleFieldStats::new);
    return singleFieldStats.getStatsForValues().computeIfAbsent(
        value,
        v -> new SingleValueStats(singleFieldStats.getStatsForValues().size(), 0)
    );
  }

  @Override
  public synchronized void addInstance(String fieldName, String value) {
    getOrCreateFor(fieldName, value).increment();
  }

  @Override
  public synchronized void shutdown() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      String fname = this.filepath.toLowerCase();
      if (!fname.endsWith(".json") && !fname.endsWith(".jsonl")) {
        throw new RuntimeException("The output file must end in .json");
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

  public NBTriesContext registerShutdownHook(NBStateContext nbctx) {
    nbctx.registerShutdownHook(this);
    return this;
  }
}
