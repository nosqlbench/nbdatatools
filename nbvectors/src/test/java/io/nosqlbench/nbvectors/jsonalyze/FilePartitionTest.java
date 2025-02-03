package io.nosqlbench.nbvectors.jsonalyze;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FilePartitionTest {

  @Test
  public void testPartitioner() {
    URL r = getClass().getClassLoader().getResource("testfiles/data.txt");
    Path path = Path.of(r.getPath());

    FilePartition p0 = FilePartition.of(path);
    List<FilePartition> p03 = p0.partition(3);
    try {
      String content = Files.readString(path);
      String actual = p03.stream().map(FilePartition::read).map(StandardCharsets.UTF_8::decode)
          .map(CharBuffer::toString).reduce("", String::concat);
      assertEquals(content, actual);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }


  }
}