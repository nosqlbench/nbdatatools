package io.nosqlbench.vectordata.spec.predicates;

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

import io.nosqlbench.vectordata.spec.metadata.MNode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for PredicateEvaluator — matching PNode predicate trees against MNode records.
class PredicateEvaluatorTest {

    // ==================== Typed int comparands ====================

    @Test
    void typedIntGT() {
        MNode record = MNode.of("age", 30L);
        PredicateNode pred = new PredicateNode("age", OpType.GT, new Comparand.IntVal(25));
        assertTrue(PredicateEvaluator.matches(pred, record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(30)), record));
    }

    @Test
    void typedIntLT() {
        MNode record = MNode.of("age", 20L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.LT, new Comparand.IntVal(25)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.LT, new Comparand.IntVal(20)), record));
    }

    @Test
    void typedIntEQ() {
        MNode record = MNode.of("age", 42L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.EQ, new Comparand.IntVal(42)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.EQ, new Comparand.IntVal(43)), record));
    }

    @Test
    void typedIntNE() {
        MNode record = MNode.of("age", 42L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.NE, new Comparand.IntVal(43)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.NE, new Comparand.IntVal(42)), record));
    }

    @Test
    void typedIntGE() {
        MNode record = MNode.of("age", 30L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.GE, new Comparand.IntVal(30)), record));
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.GE, new Comparand.IntVal(29)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.GE, new Comparand.IntVal(31)), record));
    }

    @Test
    void typedIntLE() {
        MNode record = MNode.of("age", 30L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.LE, new Comparand.IntVal(30)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.LE, new Comparand.IntVal(29)), record));
    }

    @Test
    void typedIntIN() {
        MNode record = MNode.of("age", 30L);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.IN,
                new Comparand.IntVal(10), new Comparand.IntVal(20), new Comparand.IntVal(30)),
            record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("age", OpType.IN,
                new Comparand.IntVal(10), new Comparand.IntVal(20)),
            record));
    }

    // ==================== Typed float comparands ====================

    @Test
    void typedFloatGT() {
        MNode record = MNode.of("score", 3.14);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("score", OpType.GT, new Comparand.FloatVal(3.0)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("score", OpType.GT, new Comparand.FloatVal(4.0)), record));
    }

    @Test
    void typedFloatEQ() {
        MNode record = MNode.of("score", 2.5);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("score", OpType.EQ, new Comparand.FloatVal(2.5)), record));
    }

    // ==================== Typed text comparands ====================

    @Test
    void typedTextEQ() {
        MNode record = MNode.of("name", "alice");
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice")), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("bob")), record));
    }

    @Test
    void typedTextNE() {
        MNode record = MNode.of("name", "alice");
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.NE, new Comparand.TextVal("bob")), record));
    }

    @Test
    void typedTextIN() {
        MNode record = MNode.of("color", "red");
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("color", OpType.IN,
                new Comparand.TextVal("red"), new Comparand.TextVal("blue")),
            record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("color", OpType.IN,
                new Comparand.TextVal("green"), new Comparand.TextVal("blue")),
            record));
    }

    @Test
    void typedTextMATCHES() {
        MNode record = MNode.of("name", "alice");
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.MATCHES, new Comparand.TextVal("ali.*")), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.MATCHES, new Comparand.TextVal("bob.*")), record));
    }

    @Test
    void typedTextComparison() {
        MNode record = MNode.of("name", "banana");
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.GT, new Comparand.TextVal("apple")), record));
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("name", OpType.LT, new Comparand.TextVal("cherry")), record));
    }

    // ==================== Typed bool comparands ====================

    @Test
    void typedBoolEQ() {
        MNode record = MNode.of("active", true);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("active", OpType.EQ, new Comparand.BoolVal(true)), record));
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("active", OpType.EQ, new Comparand.BoolVal(false)), record));
    }

    @Test
    void typedBoolNE() {
        MNode record = MNode.of("active", false);
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("active", OpType.NE, new Comparand.BoolVal(true)), record));
    }

    // ==================== Null field handling ====================

    @Test
    void nullFieldEqNull() {
        MNode record = MNode.of("name", "alice");
        // Field "missing" does not exist → get returns null
        assertTrue(PredicateEvaluator.matches(
            new PredicateNode("missing", OpType.EQ, new Comparand.NullVal()), record));
    }

    @Test
    void nullFieldNotMatchNonNull() {
        MNode record = MNode.of("name", "alice");
        assertFalse(PredicateEvaluator.matches(
            new PredicateNode("missing", OpType.EQ, new Comparand.IntVal(42)), record));
    }

    // ==================== Legacy i64 comparands ====================

    @Test
    void legacyLongGT() {
        MNode record = MNode.of("age", 30L);
        PredicateNode pred = new PredicateNode("age", OpType.GT, 25L);
        assertTrue(PredicateEvaluator.matches(pred, record));
    }

    @Test
    void legacyLongIN() {
        MNode record = MNode.of("age", 30L);
        PredicateNode pred = new PredicateNode("age", OpType.IN, 10L, 20L, 30L);
        assertTrue(PredicateEvaluator.matches(pred, record));
    }

    @Test
    void legacyBoolAsLong() {
        MNode record = MNode.of("flag", true);
        PredicateNode pred = new PredicateNode("flag", OpType.EQ, 1L);
        assertTrue(PredicateEvaluator.matches(pred, record));
    }

    // ==================== Conjugate nodes ====================

    @Test
    void andConjugate() {
        MNode record = MNode.of("age", 30L, "name", "alice");
        PNode<?> and = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(20)),
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice")));
        assertTrue(PredicateEvaluator.matches(and, record));
    }

    @Test
    void andConjugateShortCircuit() {
        MNode record = MNode.of("age", 30L, "name", "alice");
        PNode<?> and = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(50)),
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice")));
        assertFalse(PredicateEvaluator.matches(and, record));
    }

    @Test
    void orConjugate() {
        MNode record = MNode.of("age", 30L, "name", "bob");
        PNode<?> or = new ConjugateNode(ConjugateType.OR,
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(50)),
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("bob")));
        assertTrue(PredicateEvaluator.matches(or, record));
    }

    @Test
    void orConjugateFalse() {
        MNode record = MNode.of("age", 30L, "name", "bob");
        PNode<?> or = new ConjugateNode(ConjugateType.OR,
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(50)),
            new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice")));
        assertFalse(PredicateEvaluator.matches(or, record));
    }

    @Test
    void nestedConjugates() {
        MNode record = MNode.of("a", 10L, "b", 20L, "c", 30L);
        PNode<?> tree = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("a", OpType.EQ, new Comparand.IntVal(10)),
            new ConjugateNode(ConjugateType.OR,
                new PredicateNode("b", OpType.EQ, new Comparand.IntVal(99)),
                new PredicateNode("c", OpType.GE, new Comparand.IntVal(30))));
        assertTrue(PredicateEvaluator.matches(tree, record));
    }

    // ==================== Error cases ====================

    @Test
    void indexedPredicateThrows() {
        MNode record = MNode.of("age", 30L);
        PredicateNode indexed = new PredicateNode(0, OpType.GT, 25L);
        assertThrows(IllegalArgumentException.class,
            () -> PredicateEvaluator.matches(indexed, record));
    }
}
