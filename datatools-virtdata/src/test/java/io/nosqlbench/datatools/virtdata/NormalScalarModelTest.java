package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.vshapes.model.NormalScalarModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NormalScalarModelTest {

    @Test
    void testBasicConstruction() {
        NormalScalarModel model = new NormalScalarModel(5.0, 2.0);
        assertEquals(5.0, model.getMean());
        assertEquals(2.0, model.getStdDev());
    }

    @Test
    void testStandardNormal() {
        NormalScalarModel standard = NormalScalarModel.standardNormal();
        assertEquals(0.0, standard.getMean());
        assertEquals(1.0, standard.getStdDev());
    }

    @Test
    void testInvalidStdDev() {
        assertThrows(IllegalArgumentException.class, () -> new NormalScalarModel(0.0, 0.0));
        assertThrows(IllegalArgumentException.class, () -> new NormalScalarModel(0.0, -1.0));
    }

    @Test
    void testUniformArray() {
        NormalScalarModel[] models = NormalScalarModel.uniformScalar(1.5, 0.5, 4);
        assertEquals(4, models.length);
        for (NormalScalarModel model : models) {
            assertEquals(1.5, model.getMean());
            assertEquals(0.5, model.getStdDev());
        }
    }

    @Test
    void testRecordEquality() {
        NormalScalarModel a = new NormalScalarModel(1.0, 2.0);
        NormalScalarModel b = new NormalScalarModel(1.0, 2.0);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
