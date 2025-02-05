package io.nosqlbench.nbvectors.jjq.outputs;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

public class JsonlFileOutput implements Output {
  private final Path path;
  private final BufferedWriter writer;


  public JsonlFileOutput(Path path) {
    this.path = path;
    if (path.getParent()!=null) {
      try {
        Files.createDirectories(path.getParent(), PosixFilePermissions.asFileAttribute(
            PosixFilePermissions.fromString("rwxr-x---")
        ));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    try {
      writer.write(out.toString());
      writer.write("\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
