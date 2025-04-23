package io.nosqlbench.readers;

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


import com.google.auto.service.AutoService;
import io.nosqlbench.streamers.SizedStreamer;

import java.util.Iterator;
import java.util.NoSuchElementException;

/// A mock implementation of SizedReader for testing the SizedReaderLookup.
/// This class is included in META-INF/services to be discovered by ServiceLoader.
@AutoService(SizedStreamer.class)
@DataType(float[].class)
@Encoding(Encoding.Type.csv)
public class MockReader implements SizedStreamer<float[]> {

    @Override
    public int getSize() {
        return 10; // Mock size
    }

    @Override
    public String getName() {
        return "MockReader";
    }

    @Override
    public Iterator<float[]> iterator() {
        return new Iterator<float[]>() {
            private int current = 0;
            
            @Override
            public boolean hasNext() {
                return current < getSize();
            }

            @Override
            public float[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                float[] data = new float[3]; // Always return a vector of size 3
                data[0] = current;
                data[1] = current + 0.5f;
                data[2] = current + 1.0f;
                current++;
                return data;
            }
        };
    }
}
