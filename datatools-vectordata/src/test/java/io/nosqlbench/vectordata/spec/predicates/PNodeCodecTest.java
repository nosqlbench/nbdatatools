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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the PNode vernacular codecs: SQL, CQL, and CDDL renderers.
class PNodeCodecTest {

    // ==================== SQL Codec ====================

    @Test
    void sqlSimplePredicate() {
        PredicateNode pred = new PredicateNode("age", OpType.GT, new Comparand.IntVal(42));
        assertEquals("age > 42", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlEqualsPredicate() {
        PredicateNode pred = new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice"));
        assertEquals("name = 'alice'", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlInPredicate() {
        PredicateNode pred = new PredicateNode("id", OpType.IN,
            new Comparand.IntVal(1), new Comparand.IntVal(2), new Comparand.IntVal(3));
        assertEquals("id IN (1, 2, 3)", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlMatchesPredicate() {
        PredicateNode pred = new PredicateNode("name", OpType.MATCHES, new Comparand.TextVal("ali%"));
        assertEquals("name LIKE 'ali%'", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlBoolPredicate() {
        PredicateNode pred = new PredicateNode("active", OpType.EQ, new Comparand.BoolVal(true));
        assertEquals("active = TRUE", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlNullPredicate() {
        PredicateNode pred = new PredicateNode("value", OpType.EQ, new Comparand.NullVal());
        assertEquals("value = NULL", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlAndConjugate() {
        PNode<?> tree = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("age", OpType.GT, new Comparand.IntVal(20)),
            new PredicateNode("age", OpType.LT, new Comparand.IntVal(50)));
        assertEquals("(age > 20 AND age < 50)", PNodeSqlCodec.render(tree));
    }

    @Test
    void sqlOrConjugate() {
        PNode<?> tree = new ConjugateNode(ConjugateType.OR,
            new PredicateNode("color", OpType.EQ, new Comparand.TextVal("red")),
            new PredicateNode("color", OpType.EQ, new Comparand.TextVal("blue")));
        assertEquals("(color = 'red' OR color = 'blue')", PNodeSqlCodec.render(tree));
    }

    @Test
    void sqlLegacyLongPredicate() {
        PredicateNode pred = new PredicateNode("age", OpType.GT, 42L);
        assertEquals("age > 42", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlIndexedField() {
        PredicateNode pred = new PredicateNode(3, OpType.EQ, 100L);
        assertEquals("F3 = 100", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlTextEscaping() {
        PredicateNode pred = new PredicateNode("name", OpType.EQ, new Comparand.TextVal("O'Brien"));
        assertEquals("name = 'O''Brien'", PNodeSqlCodec.render(pred));
    }

    @Test
    void sqlFunctionInterface() {
        PNodeSqlCodec codec = new PNodeSqlCodec();
        PredicateNode pred = new PredicateNode("x", OpType.EQ, new Comparand.IntVal(1));
        assertEquals("x = 1", codec.apply(pred));
    }

    // ==================== CQL Codec ====================

    @Test
    void cqlSimplePredicate() {
        PredicateNode pred = new PredicateNode("age", OpType.GT, new Comparand.IntVal(42));
        assertEquals("age > 42", PNodeCqlCodec.render(pred));
    }

    @Test
    void cqlInPredicate() {
        PredicateNode pred = new PredicateNode("id", OpType.IN,
            new Comparand.IntVal(1), new Comparand.IntVal(2));
        assertEquals("id IN (1, 2)", PNodeCqlCodec.render(pred));
    }

    @Test
    void cqlMatchesPredicate() {
        PredicateNode pred = new PredicateNode("name", OpType.MATCHES, new Comparand.TextVal("pattern"));
        assertTrue(PNodeCqlCodec.render(pred).contains("MATCHES"));
        assertTrue(PNodeCqlCodec.render(pred).contains("pattern"));
    }

    @Test
    void cqlAndConjugateNoParens() {
        PNode<?> tree = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("a", OpType.EQ, new Comparand.IntVal(1)),
            new PredicateNode("b", OpType.EQ, new Comparand.IntVal(2)));
        String result = PNodeCqlCodec.render(tree);
        assertFalse(result.startsWith("("), "CQL should not parenthesize");
        assertTrue(result.contains("AND"));
    }

    @Test
    void cqlFunctionInterface() {
        PNodeCqlCodec codec = new PNodeCqlCodec();
        PredicateNode pred = new PredicateNode("x", OpType.EQ, new Comparand.IntVal(1));
        assertEquals("x = 1", codec.apply(pred));
    }

    // ==================== CDDL Codec ====================

    @Test
    void cddlIntPredicate() {
        PredicateNode pred = new PredicateNode("age", OpType.GT, new Comparand.IntVal(42));
        assertEquals("age: int > 42", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlFloatPredicate() {
        PredicateNode pred = new PredicateNode("score", OpType.LE, new Comparand.FloatVal(9.5));
        assertEquals("score: float <= 9.5", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlTextPredicate() {
        PredicateNode pred = new PredicateNode("name", OpType.EQ, new Comparand.TextVal("alice"));
        assertEquals("name: tstr = \"alice\"", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlBoolPredicate() {
        PredicateNode pred = new PredicateNode("active", OpType.EQ, new Comparand.BoolVal(true));
        assertEquals("active: bool = true", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlMultipleComparands() {
        PredicateNode pred = new PredicateNode("id", OpType.IN,
            new Comparand.IntVal(1), new Comparand.IntVal(2), new Comparand.IntVal(3));
        assertEquals("id: int IN [1, 2, 3]", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlAndConjugate() {
        PNode<?> tree = new ConjugateNode(ConjugateType.AND,
            new PredicateNode("a", OpType.EQ, new Comparand.IntVal(1)),
            new PredicateNode("b", OpType.EQ, new Comparand.IntVal(2)));
        assertEquals("AND { a: int = 1, b: int = 2 }", PNodeCddlCodec.render(tree));
    }

    @Test
    void cddlLegacyPredicate() {
        PredicateNode pred = new PredicateNode("age", OpType.GT, 42L);
        assertEquals("age: int > 42", PNodeCddlCodec.render(pred));
    }

    @Test
    void cddlFunctionInterface() {
        PNodeCddlCodec codec = new PNodeCddlCodec();
        PredicateNode pred = new PredicateNode("x", OpType.EQ, new Comparand.IntVal(1));
        assertEquals("x: int = 1", codec.apply(pred));
    }
}
