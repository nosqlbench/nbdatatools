package io.nosqlbench.verify_knn.datatypes;

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


import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.BitImage;
import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;


public class BitImageTest {

  @Test
  public void testBasicImage() {
    BitSet ones = new BitSet();
    BitSet zeros = new BitSet();

    BitImage image = new BitImage(zeros, ones);
    ones.set(0);
    assertThat(image.getOnes()).isEqualTo(ones);
    assertThat(image.length()).isEqualTo(1);
    zeros.set(99);
    assertThat(image.length()).isEqualTo(100);
  }

}
