package io.nosqlbench.command.build_hdf5.predicates;

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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nosqlbench.command.hdf5.subcommands.build_hdf5.predicates.PredicateParser;
import io.nosqlbench.vectordata.spec.predicates.ConjugateNode;
import io.nosqlbench.vectordata.spec.predicates.ConjugateType;
import io.nosqlbench.vectordata.spec.predicates.OpType;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PredicateParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testSimplePredicate() throws Exception {
        String json = "{\n" +
            "  \"field\": 0,\n" +
            "  \"op\": \"EQ\",\n" +
            "  \"values\": [123]\n" +
            "}";
        
        JsonNode jsonNode = MAPPER.readTree(json);
        PNode<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.field()).isEqualTo(0);
        assertThat(predicate.op()).isEqualTo(OpType.EQ);
        assertThat(predicate.v()).containsExactly(123L);
    }

    @Test
    void testInPredicate() throws Exception {
        String json = "{\n" +
            "  \"field\": 1,\n" +
            "  \"op\": \"IN\",\n" +
            "  \"values\": [3, 4, 5]\n" +
            "}";
        
        JsonNode jsonNode = MAPPER.readTree(json);
        PNode<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.field()).isEqualTo(1);
        assertThat(predicate.op()).isEqualTo(OpType.IN);
        assertThat(predicate.v()).containsExactly(3L, 4L, 5L);
    }

    @Test
    void testConjugateNode() throws Exception {
        String json = "{\n" +
            "  \"op\": \"AND\",\n" +
            "  \"nodes\": [\n" +
            "    {\n" +
            "      \"field\": 0,\n" +
            "      \"op\": \"GE\",\n" +
            "      \"values\": [100]\n" +
            "    },\n" +
            "    {\n" +
            "      \"field\": 0,\n" +
            "      \"op\": \"LE\",\n" +
            "      \"values\": [200]\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        
        JsonNode jsonNode = MAPPER.readTree(json);
        PNode<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(ConjugateNode.class);
        ConjugateNode conjugate = (ConjugateNode) node;
        assertThat(conjugate.type()).isEqualTo(ConjugateType.AND);
        assertThat(conjugate.values()).hasSize(2);
        
        PredicateNode first = (PredicateNode) conjugate.values()[0];
        assertThat(first.field()).isEqualTo(0);
        assertThat(first.op()).isEqualTo(OpType.GE);
        assertThat(first.v()).containsExactly(100L);
        
        PredicateNode second = (PredicateNode) conjugate.values()[1];
        assertThat(second.field()).isEqualTo(0);
        assertThat(second.op()).isEqualTo(OpType.LE);
        assertThat(second.v()).containsExactly(200L);
    }

    @Test
    void testValidation() throws Exception {
        // Missing required field
        String missingField = "{\n" +
            "  \"type\": \"predicate\",\n" +
            "  \"op\": \"EQ\",\n" +
            "  \"values\": [123]\n" +
            "}";
        JsonNode jsonNode = MAPPER.readTree(missingField);
        JsonNode finalJsonNode = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required field: field");

        // Invalid operator
        String invalidOperator = "{\n" +
            "  \"type\": \"predicate\",\n" +
            "  \"field\": 0,\n" +
            "  \"op\": \"INVALID\",\n" +
            "  \"values\": [123]\n" +
            "}";
        jsonNode = MAPPER.readTree(invalidOperator);
        JsonNode finalJsonNode1 = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown operator: INVALID");

        // Multiple values for non-IN predicate
        String multipleValues = "{\n" +
            "  \"type\": \"predicate\",\n" +
            "  \"field\": 0,\n" +
            "  \"op\": \"EQ\",\n" +
            "  \"values\": [1, 2]\n" +
            "}";
        jsonNode = MAPPER.readTree(multipleValues);
        JsonNode finalJsonNode2 = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Non-IN predicates must have exactly one value");
    }

    @Test
    void testOperatorSymbols() throws Exception {
        String json = "{\n" +
            "  \"type\": \"predicate\",\n" +
            "  \"field\": 0,\n" +
            "  \"op\": \">=\",\n" +
            "  \"values\": [100]\n" +
            "}";
        
        JsonNode jsonNode = MAPPER.readTree(json);
        PNode<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.op()).isEqualTo(OpType.GE);
        
        // Test with symbol "<"
        json = "{\n" +
            "  \"type\": \"predicate\",\n" +
            "  \"field\": 0,\n" +
            "  \"op\": \"<\",\n" +
            "  \"values\": [100]\n" +
            "}";
        
        jsonNode = MAPPER.readTree(json);
        node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        predicate = (PredicateNode) node;
        assertThat(predicate.op()).isEqualTo(OpType.LT);
    }
}
