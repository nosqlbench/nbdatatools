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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TerminalColorSupportTest {

    @Test
    public void testDetectColorDepthWithNoEnvVars() {
        // When no environment variables are set
        Map<String, String> emptyEnv = new HashMap<>();
        assertThat(TerminalColorSupport.detectColorDepth(emptyEnv)).isEqualTo(ColorDepth.NOCOLOR);
    }

    @Test
    public void testDetectColorDepthWithColorTerm() {
        Map<String, String> env = new HashMap<>();
        
        // Test with COLORTERM=truecolor
        env.put("COLORTERM", "truecolor");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI24BITCOLOR);
        
        // Test with COLORTERM=24bit
        env.clear();
        env.put("COLORTERM", "24bit");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI24BITCOLOR);
        
        // Test with COLORTERM=256color
        env.clear();
        env.put("COLORTERM", "256color");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI256COLOR);
        
        // Test with COLORTERM=other
        env.clear();
        env.put("COLORTERM", "other");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI8COLOR);
    }

    @Test
    public void testDetectColorDepthWithTerm() {
        Map<String, String> env = new HashMap<>();
        
        // Test with TERM=xterm-256color
        env.put("TERM", "xterm-256color");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI256COLOR);
        
        // Test with TERM=xterm-color
        env.clear();
        env.put("TERM", "xterm-color");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI8COLOR);
        
        // Test with TERM=dumb
        env.clear();
        env.put("TERM", "dumb");
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.NOCOLOR);
    }

    @Test
    public void testColorTermTakesPrecedenceOverTerm() {
        // Set both COLORTERM and TERM
        Map<String, String> env = new HashMap<>();
        env.put("COLORTERM", "256color");
        env.put("TERM", "xterm-24bit");
        
        // COLORTERM should take precedence
        assertThat(TerminalColorSupport.detectColorDepth(env)).isEqualTo(ColorDepth.ANSI256COLOR);
    }

    @Test
    public void testGetCurrentColorDepth() {
        // This test is more of a smoke test since we can't control the actual environment
        // Just verify that getCurrentColorDepth() returns a valid ColorDepth value
        ColorDepth depth = TerminalColorSupport.getCurrentColorDepth();
        assertThat(depth).isNotNull();
    }
    
    @Test
    public void testFunctionAccessor() {
        // Test using a function accessor instead of a map
        assertThat(TerminalColorSupport.detectColorDepth(key -> {
            if ("COLORTERM".equals(key)) return "truecolor";
            return null;
        })).isEqualTo(ColorDepth.ANSI24BITCOLOR);
        
        assertThat(TerminalColorSupport.detectColorDepth(key -> {
            if ("TERM".equals(key)) return "xterm-256color";
            return null;
        })).isEqualTo(ColorDepth.ANSI256COLOR);
    }
}