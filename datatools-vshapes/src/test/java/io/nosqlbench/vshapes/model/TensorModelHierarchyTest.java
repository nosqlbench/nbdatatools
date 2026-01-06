package io.nosqlbench.vshapes.model;

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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the tensor model type hierarchy.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>ScalarModel is the base interface for all distribution models</li>
 *   <li>VectorModel is implemented by VectorSpaceModel</li>
 *   <li>ScalarModel extends ScalarModel (backward compatibility)</li>
 *   <li>New *ScalarModel classes work correctly</li>
 * </ul>
 */
class TensorModelHierarchyTest {

    // ===== ScalarModel Interface Tests =====

    @Test
    void scalarModelInterfaceIsImplementedByNormalScalarModel() {
        ScalarModel scalar = new NormalScalarModel(0.0, 1.0);
        assertNotNull(scalar.getModelType());
        assertEquals("normal", scalar.getModelType());
    }

    @Test
    void scalarModelInterfaceIsImplementedByUniformScalarModel() {
        ScalarModel scalar = new UniformScalarModel(0.0, 1.0);
        assertNotNull(scalar.getModelType());
        assertEquals("uniform", scalar.getModelType());
    }

    @Test
    void scalarModelInterfaceIsImplementedByEmpiricalScalarModel() {
        float[] data = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
        ScalarModel scalar = EmpiricalScalarModel.fromData(data);
        assertNotNull(scalar.getModelType());
        assertEquals("empirical", scalar.getModelType());
    }

    @Test
    void scalarModelInterfaceIsImplementedByCompositeScalarModel() {
        ScalarModel scalar = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(0.0, 1.0),
                new UniformScalarModel(-1.0, 1.0)
            ),
            new double[]{0.5, 0.5}
        );
        assertNotNull(scalar.getModelType());
        assertEquals("composite", scalar.getModelType());
    }

    // ===== ScalarModel Extends ScalarModel (Backward Compatibility) =====

    @Test
    
    void componentModelExtendsScalarModel() {
        ScalarModel component = new NormalScalarModel(0.0, 1.0);
        assertTrue(component instanceof ScalarModel);
    }

    @Test
    
    void normalComponentModelImplementsScalarModel() {
        NormalScalarModel normal = new NormalScalarModel(0.0, 1.0);
        ScalarModel scalar = normal;
        assertEquals("normal", scalar.getModelType());
    }

    @Test
    
    void uniformComponentModelImplementsScalarModel() {
        UniformScalarModel uniform = new UniformScalarModel(0.0, 1.0);
        ScalarModel scalar = uniform;
        assertEquals("uniform", scalar.getModelType());
    }

    // ===== VectorModel Interface Tests =====

    @Test
    void vectorSpaceModelImplementsVectorModel() {
        VectorModel vector = new VectorSpaceModel(1_000_000, 128);
        assertEquals(1_000_000L, vector.uniqueVectors());
        assertEquals(128, vector.dimensions());
        assertNotNull(vector.scalarModel(0));
        assertNotNull(vector.scalarModels());
    }

    @Test
    void vectorModelScalarModelsReturnsCorrectCount() {
        VectorModel vector = new VectorSpaceModel(1000, 4);
        ScalarModel[] scalars = vector.scalarModels();
        assertEquals(4, scalars.length);
    }

    @Test
    
    void vectorModelScalarModelReturnsCorrectModel() {
        ScalarModel[] custom = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(0.0, 1.0),
            new NormalScalarModel(-5.0, 0.5)
        };
        VectorModel vector = new VectorSpaceModel(1000, custom);

        assertEquals("normal", vector.scalarModel(0).getModelType());
        assertEquals("uniform", vector.scalarModel(1).getModelType());
        assertEquals("normal", vector.scalarModel(2).getModelType());
    }

    @Test
    
    void vectorModelWithHeterogeneousScalarModels() {
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0),
            new NormalScalarModel(5.0, 2.0)
        };
        VectorSpaceModel vsm = new VectorSpaceModel(100, models);
        VectorModel vector = vsm;

        assertEquals(3, vector.dimensions());
        assertInstanceOf(NormalScalarModel.class, vector.scalarModel(0));
        assertInstanceOf(UniformScalarModel.class, vector.scalarModel(1));
        assertInstanceOf(NormalScalarModel.class, vector.scalarModel(2));
    }

    // ===== New *ScalarModel Class Tests =====

    @Test
    void normalScalarModelExtendsNormalScalarModel() {
        NormalScalarModel scalar = new NormalScalarModel(0.0, 1.0);
        assertTrue(scalar instanceof NormalScalarModel);
        assertEquals(0.0, scalar.getMean());
        assertEquals(1.0, scalar.getStdDev());
    }

    @Test
    void normalScalarModelTruncated() {
        NormalScalarModel scalar = new NormalScalarModel(0.0, 1.0, -1.0, 1.0);
        assertTrue(scalar.isTruncated());
        assertEquals(-1.0, scalar.lower());
        assertEquals(1.0, scalar.upper());
    }

    @Test
    void uniformScalarModelExtendsUniformScalarModel() {
        UniformScalarModel scalar = new UniformScalarModel(0.0, 1.0);
        assertTrue(scalar instanceof UniformScalarModel);
        assertEquals(0.0, scalar.getLower());
        assertEquals(1.0, scalar.getUpper());
    }

    @Test
    void empiricalScalarModelExtendsEmpiricalScalarModel() {
        float[] data = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
        EmpiricalScalarModel scalar = EmpiricalScalarModel.fromData(data);
        assertTrue(scalar instanceof EmpiricalScalarModel);
        assertTrue(scalar.getBinCount() > 0);
    }

    @Test
    void compositeScalarModelExtendsCompositeScalarModel() {
        CompositeScalarModel scalar = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(0.0, 1.0),
                new UniformScalarModel(-1.0, 1.0)
            ),
            new double[]{0.5, 0.5}
        );
        assertTrue(scalar instanceof CompositeScalarModel);
        assertEquals(2, scalar.getComponentCount());
    }

    @Test
    void compositeScalarModelWithWeights() {
        CompositeScalarModel scalar = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(0.0, 1.0),
                new UniformScalarModel(-1.0, 1.0)
            ),
            new double[]{0.7, 0.3}
        );
        double[] weights = scalar.getWeights();
        assertEquals(0.7, weights[0], 0.001);
        assertEquals(0.3, weights[1], 0.001);
    }

    // ===== Static Factory Method Tests =====

    @Test
    void normalScalarModelUniformFactory() {
        NormalScalarModel[] models = NormalScalarModel.uniformScalar(0.0, 1.0, 4);
        assertEquals(4, models.length);
        for (NormalScalarModel model : models) {
            assertEquals(0.0, model.getMean());
            assertEquals(1.0, model.getStdDev());
        }
    }

    @Test
    void uniformScalarModelUniformFactory() {
        UniformScalarModel[] models = UniformScalarModel.uniformScalar(-1.0, 1.0, 3);
        assertEquals(3, models.length);
        for (UniformScalarModel model : models) {
            assertEquals(-1.0, model.getLower());
            assertEquals(1.0, model.getUpper());
        }
    }

    // ===== Type Casting Tests =====

    @Test
    void scalarModelCanBeCastToComponentModel() {
        ScalarModel scalar = new NormalScalarModel(0.0, 1.0);
        if (scalar instanceof ScalarModel component) {
            assertEquals("normal", component.getModelType());
        } else {
            fail("NormalScalarModel should be castable to ScalarModel");
        }
    }

    @Test
    void vectorModelCanBeCastToVectorSpaceModel() {
        VectorModel vector = new VectorSpaceModel(1000, 128);
        if (vector instanceof VectorSpaceModel vsm) {
            assertEquals(128, vsm.dimensions());
            assertTrue(vsm.isAllNormal());
        } else {
            fail("VectorSpaceModel should be castable from VectorModel");
        }
    }

    // ===== IsomorphicVectorModel Interface Tests =====

    @Test
    void vectorSpaceModelImplementsIsomorphicVectorModel() {
        VectorSpaceModel vsm = new VectorSpaceModel(1000, 128);
        assertTrue(vsm instanceof IsomorphicVectorModel);
    }

    @Test
    void isomorphicVectorModelWithUniformGaussian() {
        VectorSpaceModel vsm = new VectorSpaceModel(1000, 128, 0.0, 1.0);
        IsomorphicVectorModel ivm = vsm;

        assertTrue(ivm.isIsomorphic());
        assertEquals("normal", ivm.isomorphicModelType());
        assertNotNull(ivm.representativeScalarModel());
        assertEquals(NormalScalarModel.class, ivm.scalarModelClass());
    }

    @Test
    void isomorphicVectorModelWithUniformUniform() {
        ScalarModel[] models = UniformScalarModel.uniformScalar(-1.0, 1.0, 64);
        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        IsomorphicVectorModel ivm = vsm;

        assertTrue(ivm.isIsomorphic());
        assertEquals("uniform", ivm.isomorphicModelType());
        assertInstanceOf(UniformScalarModel.class, ivm.representativeScalarModel());
        assertEquals(UniformScalarModel.class, ivm.scalarModelClass());
    }

    @Test
    
    void heterogeneousVectorModelIsNotIsomorphic() {
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0),
            new NormalScalarModel(5.0, 2.0)
        };
        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        IsomorphicVectorModel ivm = vsm;

        assertFalse(ivm.isIsomorphic());
    }

    @Test
    
    void heterogeneousModelThrowsOnRepresentativeModel() {
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0)
        };
        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        IsomorphicVectorModel ivm = vsm;

        assertThrows(IllegalStateException.class, ivm::representativeScalarModel);
    }

    @Test
    
    void heterogeneousModelThrowsOnScalarModelClass() {
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0)
        };
        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        IsomorphicVectorModel ivm = vsm;

        assertThrows(IllegalStateException.class, ivm::scalarModelClass);
    }

    @Test
    
    void heterogeneousModelThrowsOnIsomorphicModelType() {
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0)
        };
        VectorSpaceModel vsm = new VectorSpaceModel(1000, models);
        IsomorphicVectorModel ivm = vsm;

        assertThrows(IllegalStateException.class, ivm::isomorphicModelType);
    }

    @Test
    void samplerResolverPatternWithIsomorphicModel() {
        // Demonstrates the sampler resolver use case
        VectorModel model = new VectorSpaceModel(1000, 128, 0.0, 1.0);

        String samplerType = resolveSamplerType(model);
        assertEquals("vectorized-normal", samplerType);
    }

    @Test
    
    void samplerResolverPatternWithHeterogeneousModel() {
        // Demonstrates fallback for heterogeneous models
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(-1.0, 1.0)
        };
        VectorModel model = new VectorSpaceModel(1000, models);

        String samplerType = resolveSamplerType(model);
        assertEquals("component-wise", samplerType);
    }

    /// Example sampler resolver method demonstrating the interface usage
    private String resolveSamplerType(VectorModel model) {
        if (model instanceof IsomorphicVectorModel ivm && ivm.isIsomorphic()) {
            Class<?> type = ivm.scalarModelClass();
            if (NormalScalarModel.class.isAssignableFrom(type)) {
                return "vectorized-normal";
            } else if (UniformScalarModel.class.isAssignableFrom(type)) {
                return "vectorized-uniform";
            }
            return "vectorized-generic";
        }
        return "component-wise";
    }
}
