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


import io.nosqlbench.nbvectors.commands.jjq.bulkio.LineChunker;
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
