package io.nosqlbench.nbdatatools.api.types.terminal;

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

public class ColorDepthTest {

    @Test
    public void testSupportsColor() {
        assertThat(ColorDepth.NOCOLOR.supportsColor()).isFalse();
        assertThat(ColorDepth.ANSI8COLOR.supportsColor()).isTrue();
        assertThat(ColorDepth.ANSI256COLOR.supportsColor()).isTrue();
        assertThat(ColorDepth.ANSI24BITCOLOR.supportsColor()).isTrue();
    }

    @Test
    public void testSupports256Colors() {
        assertThat(ColorDepth.NOCOLOR.supports256Colors()).isFalse();
        assertThat(ColorDepth.ANSI8COLOR.supports256Colors()).isFalse();
        assertThat(ColorDepth.ANSI256COLOR.supports256Colors()).isTrue();
        assertThat(ColorDepth.ANSI24BITCOLOR.supports256Colors()).isTrue();
    }

    @Test
    public void testSupportsTrueColor() {
        assertThat(ColorDepth.NOCOLOR.supportsTrueColor()).isFalse();
        assertThat(ColorDepth.ANSI8COLOR.supportsTrueColor()).isFalse();
        assertThat(ColorDepth.ANSI256COLOR.supportsTrueColor()).isFalse();
        assertThat(ColorDepth.ANSI24BITCOLOR.supportsTrueColor()).isTrue();
    }
}