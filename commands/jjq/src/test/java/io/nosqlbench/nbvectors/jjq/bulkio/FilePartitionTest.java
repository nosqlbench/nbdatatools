package io.nosqlbench.nbvectors.jjq.bulkio;

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


import io.nosqlbench.command.jjq.bulkio.FilePartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilePartitionTest {

  @Test
  public void testPartitioner() {
    URL r = getClass().getClassLoader().getResource("testfiles/data.txt");
    Path path = Path.of(r.getPath());

    FilePartition p0 = FilePartition.of(path);
    List<FilePartition> p03 = p0.partition(3);
    try {
      String content = Files.readString(path);
      String actual = p03.stream().map(FilePartition::mapFile).map(StandardCharsets.UTF_8::decode)
          .map(CharBuffer::toString).reduce("", String::concat);
      assertEquals(content, actual);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }


  }
}
