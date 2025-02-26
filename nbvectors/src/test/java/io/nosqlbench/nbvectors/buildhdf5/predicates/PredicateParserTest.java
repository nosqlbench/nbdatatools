package io.nosqlbench.nbvectors.buildhdf5.predicates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.*;
import io.nosqlbench.nbvectors.predicates.PredicateParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PredicateParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testSimplePredicate() throws Exception {
        String json = """
            {
              "field": 0,
              "operator": "EQ",
              "values": [123]
            }
            """;
        
        JsonNode jsonNode = MAPPER.readTree(json);
        Node<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.field()).isEqualTo(0);
        assertThat(predicate.op()).isEqualTo(OpType.EQ);
        assertThat(predicate.v()).containsExactly(123L);
    }

    @Test
    void testInPredicate() throws Exception {
        String json = """
            {
              "field": 1,
              "operator": "IN",
              "values": [3, 4, 5]
            }
            """;
        
        JsonNode jsonNode = MAPPER.readTree(json);
        Node<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.field()).isEqualTo(1);
        assertThat(predicate.op()).isEqualTo(OpType.IN);
        assertThat(predicate.v()).containsExactly(3L, 4L, 5L);
    }

    @Test
    void testConjugateNode() throws Exception {
        String json = """
            {
              "operator": "AND",
              "nodes": [
                {
                  "field": 0,
                  "operator": "GE",
                  "values": [100]
                },
                {
                  "field": 0,
                  "operator": "LE",
                  "values": [200]
                }
              ]
            }
            """;
        
        JsonNode jsonNode = MAPPER.readTree(json);
        Node<?> node = PredicateParser.parse(jsonNode);
        
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
        String missingField = """
            {
              "type": "predicate",
              "operator": "EQ",
              "values": [123]
            }
            """;
        JsonNode jsonNode = MAPPER.readTree(missingField);
        JsonNode finalJsonNode = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required field: field");

        // Invalid operator
        String invalidOperator = """
            {
              "type": "predicate",
              "field": 0,
              "operator": "INVALID",
              "values": [123]
            }
            """;
        jsonNode = MAPPER.readTree(invalidOperator);
        JsonNode finalJsonNode1 = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown operator: INVALID");

        // Multiple values for non-IN predicate
        String multipleValues = """
            {
              "type": "predicate",
              "field": 0,
              "operator": "EQ",
              "values": [1, 2]
            }
            """;
        jsonNode = MAPPER.readTree(multipleValues);
        JsonNode finalJsonNode2 = jsonNode;
        assertThatThrownBy(() -> PredicateParser.parse(finalJsonNode2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Non-IN predicates must have exactly one value");
    }

    @Test
    void testOperatorSymbols() throws Exception {
        String json = """
            {
              "type": "predicate",
              "field": 0,
              "operator": ">=",
              "values": [100]
            }
            """;
        
        JsonNode jsonNode = MAPPER.readTree(json);
        Node<?> node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        PredicateNode predicate = (PredicateNode) node;
        assertThat(predicate.op()).isEqualTo(OpType.GE);
        
        // Test with symbol "<"
        json = """
            {
              "type": "predicate",
              "field": 0,
              "operator": "<",
              "values": [100]
            }
            """;
        
        jsonNode = MAPPER.readTree(json);
        node = PredicateParser.parse(jsonNode);
        
        assertThat(node).isInstanceOf(PredicateNode.class);
        predicate = (PredicateNode) node;
        assertThat(predicate.op()).isEqualTo(OpType.LT);
    }
}
