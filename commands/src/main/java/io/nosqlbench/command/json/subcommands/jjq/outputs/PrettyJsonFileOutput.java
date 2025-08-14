package io.nosqlbench.command.json.subcommands.jjq.outputs;

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


import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

/// An implementation of output which writes all nodes to a file with {@link JsonNode#toPrettyString()}
public class PrettyJsonFileOutput implements Output {
  private final Path path;
  private final BufferedWriter writer;

  /// create a pretty json file output
  /// @param path the path to the file to write to
  public PrettyJsonFileOutput(Path path) {
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
      writer.write(out.toPrettyString());
      writer.write("\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
