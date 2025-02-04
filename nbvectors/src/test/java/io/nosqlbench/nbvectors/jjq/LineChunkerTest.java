package io.nosqlbench.nbvectors.jjq;

import io.nosqlbench.nbvectors.jjq.bulkio.LineChunker;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class LineChunkerTest {

  @Test
  public void testBasicChunking() {
    URL r = getClass().getClassLoader().getResource("testfiles/data.txt");
    Path p = Path.of(r.getPath());
    Iterator<CharBuffer> lineChunker = new LineChunker(p, 0, 3).iterator();
    assertTrue(lineChunker.hasNext());
    assertThat(lineChunker.next().toString()).isEqualTo("""
        one two three four five
        six seven eight nine ten
        eleven twelve thirteen fourteen fifteen""");
    assertTrue(lineChunker.hasNext());
    assertThat(lineChunker.next().toString()).isEqualTo("""
        sixteen seventeen eighteen nineteen twenty
        twentyone twentytwo twentythree twentyfour twentyfive""");
    assertTrue(lineChunker.hasNext());
    CharBuffer next = lineChunker.next();
    assertThat(next.toString()).isEqualTo("""
        twentysix twentyseven twentyeight twentynine thirty""");

  }


}