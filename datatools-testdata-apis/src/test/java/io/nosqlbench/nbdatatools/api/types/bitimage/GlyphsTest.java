package io.nosqlbench.nbdatatools.api.types.bitimage;

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


import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GlyphsTest {


  @Test
  public void testGlyphLookup() {
    assertThat(Glyphs.chars[0]).isEqualTo('\u2800');
    assertThat(Glyphs.chars[129]).isEqualTo('⢁');
    assertThat(Glyphs.chars[255]).isEqualTo('⣿');
  }

  @Test
  // Test that the mask applies and returns the correct character
  public void testOrMask() {
    assertThat(Glyphs.orMask(Glyphs.chars[1],2)).isEqualTo(Glyphs.chars[3]);
    assertThat(Glyphs.orMask(Glyphs.chars[137],137)).isEqualTo(Glyphs.chars[137]);
    assertThat(Glyphs.orMask(Glyphs.chars[15],240)).isEqualTo(Glyphs.chars[255]);
  }
}
