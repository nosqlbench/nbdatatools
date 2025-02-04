package io.nosqlbench.nbvectors.jjq;

import io.nosqlbench.nbvectors.jjq.bulkio.CharbufChunker;
import org.junit.jupiter.api.Test;

import java.nio.CharBuffer;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

public class CharbufChunkerTest {

  @Test
  public void testChunker() {
    ///                                   0         10        20        30        40        50
    ///                                   0123456789012345678901234567890123456789012345678901234
    CharBuffer buf = CharBuffer.wrap("one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten");
    CharbufChunker chunker = new CharbufChunker(buf, 10);
    Iterator<CharBuffer> iter = chunker.iterator();
    assertThat(iter.hasNext()).isTrue();
    assertThat(iter.next().toString()).isEqualTo("""
        one
        two
        three
        """);
    assertThat(iter.hasNext()).isTrue();
    assertThat(iter.next().toString()).isEqualTo("""
        four
        five
        six
        """);
    assertThat(iter.hasNext()).isTrue();
    assertThat(iter.next().toString()).isEqualTo("""
        seven
        eight
        """);
    assertThat(iter.hasNext()).isTrue();
    assertThat(iter.next().toString()).isEqualTo("""
        nine
        ten""");

  }

}