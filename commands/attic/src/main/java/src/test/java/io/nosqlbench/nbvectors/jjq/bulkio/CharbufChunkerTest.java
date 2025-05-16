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


import io.nosqlbench.nbvectors.commands.jjq.bulkio.CharbufChunker;
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
