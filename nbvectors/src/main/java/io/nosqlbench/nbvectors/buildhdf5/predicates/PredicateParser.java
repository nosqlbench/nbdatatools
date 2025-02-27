package io.nosqlbench.nbvectors.buildhdf5.predicates;

import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.ConjugateNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.ConjugateType;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.OpType;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PredicateNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PredicateParser {
    private static final Set<String> CONJUGATE_OPS = Set.of("AND", "OR");
    
    public static PNode<?> parse(JsonNode root) {
        validateRequiredField(root, "op");
        String operator = root.get("op").asText().toUpperCase();
        
        // Explicitly check if this is a conjugate operator
        if (CONJUGATE_OPS.contains(operator)) {
            return parseConjugate(root);
        } else {
            return parsePredicate(root);
        }
    }
    
    private static PredicateNode parsePredicate(JsonNode node) {
        validatePredicateNode(node);
        
        int field = node.get("field").asInt();
        OpType operator = parseOperator(node.get("op").asText());
        long[] values = parseValues(node.get("values"));
        
        if (operator != OpType.IN && values.length != 1) {
            throw new IllegalArgumentException("Non-IN predicates must have exactly one value");
        }
        
        return new PredicateNode(field, operator, values);
    }
    
    private static ConjugateNode parseConjugate(JsonNode node) {
        validateConjugateNode(node);
        
        ConjugateType type = parseConjugateType(node.get("op").asText());
        JsonNode nodesArray = node.get("nodes");
        
        if (!nodesArray.isArray()) {
            throw new IllegalArgumentException("nodes must be an array");
        }
        
        List<PNode<?>> nodes = new ArrayList<>();
        for (JsonNode childNode : nodesArray) {
            nodes.add(parse(childNode));
        }
        
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Conjugate node must have at least one child node");
        }
        
        return new ConjugateNode(type, nodes.toArray(new PNode<?>[0]));
    }
    
    private static OpType parseOperator(String op) {
        String upperOp = op.toUpperCase();
        
        // First check if it's a valid enum name
        for (OpType type : OpType.values()) {
            if (type.name().equals(upperOp)) {
                return type;
            }
        }
        
        // Then check if it matches a symbol
        for (OpType type : OpType.values()) {
            if (type.symbol().equals(op)) {
                return type;
            }
        }
        
        throw new IllegalArgumentException("Unknown operator: " + op);
    }
    
    private static ConjugateType parseConjugateType(String op) {
        return switch (op.toUpperCase()) {
            case "AND" -> ConjugateType.AND;
            case "OR" -> ConjugateType.OR;
            default -> throw new IllegalArgumentException("Unknown conjugate type: " + op);
        };
    }
    
    private static long[] parseValues(JsonNode valuesNode) {
        if (!valuesNode.isArray()) {
            throw new IllegalArgumentException("values must be an array");
        }
        
        long[] values = new long[valuesNode.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = valuesNode.get(i).asLong();
        }
        return values;
    }
    
    private static void validatePredicateNode(JsonNode node) {
        validateRequiredField(node, "field");
        validateRequiredField(node, "op");
        validateRequiredField(node, "values");
        
        if (!node.get("field").isNumber()) {
            throw new IllegalArgumentException("field must be a number");
        }
        if (node.get("field").asInt() < 0) {
            throw new IllegalArgumentException("field must be non-negative");
        }
    }
    
    private static void validateConjugateNode(JsonNode node) {
        validateRequiredField(node, "op");
        validateRequiredField(node, "nodes");
    }
    
    private static void validateRequiredField(JsonNode node, String fieldName) {
        if (!node.has(fieldName)) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
    }
}