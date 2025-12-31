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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GaussianComponentModelTest {

    @Test
    void testBasicConstruction() {
        GaussianComponentModel model = new GaussianComponentModel(5.0, 2.0);
        assertEquals(5.0, model.mean());
        assertEquals(2.0, model.stdDev());
    }

    @Test
    void testStandardNormal() {
        GaussianComponentModel standard = GaussianComponentModel.standardNormal();
        assertEquals(0.0, standard.mean());
        assertEquals(1.0, standard.stdDev());
    }

    @Test
    void testInvalidStdDev() {
        assertThrows(IllegalArgumentException.class, () -> new GaussianComponentModel(0.0, 0.0));
        assertThrows(IllegalArgumentException.class, () -> new GaussianComponentModel(0.0, -1.0));
    }

    @Test
    void testUniformArray() {
        GaussianComponentModel[] models = GaussianComponentModel.uniform(1.5, 0.5, 4);
        assertEquals(4, models.length);
        for (GaussianComponentModel model : models) {
            assertEquals(1.5, model.mean());
            assertEquals(0.5, model.stdDev());
        }
    }

    @Test
    void testRecordEquality() {
        GaussianComponentModel a = new GaussianComponentModel(1.0, 2.0);
        GaussianComponentModel b = new GaussianComponentModel(1.0, 2.0);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
