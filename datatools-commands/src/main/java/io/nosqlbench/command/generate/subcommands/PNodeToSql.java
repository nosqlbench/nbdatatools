package io.nosqlbench.command.generate.subcommands;

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

import io.nosqlbench.vectordata.discovery.metadata.FieldDescriptor;
import io.nosqlbench.vectordata.discovery.metadata.FieldType;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.spec.predicates.ConjugateNode;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;

import java.util.Map;
import java.util.StringJoiner;

/// Converts a {@link PNode} predicate tree into a SQL WHERE clause string.
///
/// For TEXT fields, the long values stored in {@link PredicateNode} are resolved
/// to string literals using a provided ordinal-to-string mapping.
///
/// For INT fields, the long values are used directly.
///
/// For FLOAT fields, the long values are interpreted as
/// {@link Double#longBitsToDouble(long)} encoded doubles.
public final class PNodeToSql {

    private PNodeToSql() {}

    /// Converts a predicate tree to a SQL WHERE clause.
    ///
    /// @param node          the root of the predicate tree
    /// @param layout        the metadata layout for resolving field names
    /// @param stringTable   mapping from long ordinal to string value for TEXT fields
    /// @return the SQL WHERE clause (without the WHERE keyword)
    public static String toSql(PNode<?> node, MetadataLayout layout, Map<Long, String> stringTable) {
        if (node instanceof PredicateNode pred) {
            return predicateToSql(pred, layout, stringTable);
        } else if (node instanceof ConjugateNode conj) {
            return conjugateToSql(conj, layout, stringTable);
        }
        throw new IllegalArgumentException("Unknown PNode type: " + node.getClass());
    }

    /// Converts a predicate tree to a SQL WHERE clause using a {@link PredicateContext}
    /// for field resolution.
    ///
    /// @param node          the root of the predicate tree
    /// @param ctx           the predicate context for resolving field descriptors
    /// @param stringTable   mapping from long ordinal to string value for TEXT fields
    /// @return the SQL WHERE clause (without the WHERE keyword)
    public static String toSql(PNode<?> node, PredicateContext ctx, Map<Long, String> stringTable) {
        if (node instanceof PredicateNode pred) {
            return predicateToSql(pred, ctx, stringTable);
        } else if (node instanceof ConjugateNode conj) {
            return conjugateToSql(conj, ctx, stringTable);
        }
        throw new IllegalArgumentException("Unknown PNode type: " + node.getClass());
    }

    private static String predicateToSql(PredicateNode pred, PredicateContext ctx,
                                         Map<Long, String> stringTable) {
        FieldDescriptor fd = ctx.fieldDescriptor(pred);
        String fieldName = fd.name();
        long[] values = pred.v();

        return switch (pred.op()) {
            case GT -> fieldName + " > " + formatValue(fd, values[0], stringTable);
            case LT -> fieldName + " < " + formatValue(fd, values[0], stringTable);
            case EQ -> fieldName + " = " + formatValue(fd, values[0], stringTable);
            case NE -> fieldName + " != " + formatValue(fd, values[0], stringTable);
            case GE -> fieldName + " >= " + formatValue(fd, values[0], stringTable);
            case LE -> fieldName + " <= " + formatValue(fd, values[0], stringTable);
            case IN -> {
                StringJoiner sj = new StringJoiner(", ", fieldName + " IN (", ")");
                for (long v : values) {
                    sj.add(formatValue(fd, v, stringTable));
                }
                yield sj.toString();
            }
            case MATCHES -> fieldName + " GLOB " + formatValue(fd, values[0], stringTable);
        };
    }

    private static String conjugateToSql(ConjugateNode conj, PredicateContext ctx,
                                         Map<Long, String> stringTable) {
        String joiner = switch (conj.type()) {
            case AND -> " AND ";
            case OR -> " OR ";
            default -> throw new IllegalArgumentException("Unexpected conjugate type: " + conj.type());
        };

        StringJoiner sj = new StringJoiner(joiner, "(", ")");
        for (PNode<?> child : conj.values()) {
            sj.add(toSql(child, ctx, stringTable));
        }
        return sj.toString();
    }

    private static String predicateToSql(PredicateNode pred, MetadataLayout layout,
                                         Map<Long, String> stringTable) {
        FieldDescriptor fd = layout.getField(pred.field());
        String fieldName = fd.name();
        long[] values = pred.v();

        return switch (pred.op()) {
            case GT -> fieldName + " > " + formatValue(fd, values[0], stringTable);
            case LT -> fieldName + " < " + formatValue(fd, values[0], stringTable);
            case EQ -> fieldName + " = " + formatValue(fd, values[0], stringTable);
            case NE -> fieldName + " != " + formatValue(fd, values[0], stringTable);
            case GE -> fieldName + " >= " + formatValue(fd, values[0], stringTable);
            case LE -> fieldName + " <= " + formatValue(fd, values[0], stringTable);
            case IN -> {
                StringJoiner sj = new StringJoiner(", ", fieldName + " IN (", ")");
                for (long v : values) {
                    sj.add(formatValue(fd, v, stringTable));
                }
                yield sj.toString();
            }
            case MATCHES -> fieldName + " GLOB " + formatValue(fd, values[0], stringTable);
        };
    }

    private static String conjugateToSql(ConjugateNode conj, MetadataLayout layout,
                                         Map<Long, String> stringTable) {
        String joiner = switch (conj.type()) {
            case AND -> " AND ";
            case OR -> " OR ";
            default -> throw new IllegalArgumentException("Unexpected conjugate type: " + conj.type());
        };

        StringJoiner sj = new StringJoiner(joiner, "(", ")");
        for (PNode<?> child : conj.values()) {
            sj.add(toSql(child, layout, stringTable));
        }
        return sj.toString();
    }

    private static String formatValue(FieldDescriptor fd, long value, Map<Long, String> stringTable) {
        return switch (fd.type()) {
            case TEXT -> {
                String s = stringTable.get(value);
                if (s == null) {
                    throw new IllegalArgumentException(
                        "No string table entry for ordinal " + value + " on field " + fd.name());
                }
                yield "'" + s.replace("'", "''") + "'";
            }
            case INT -> String.valueOf(value);
            case FLOAT -> String.valueOf(Double.longBitsToDouble(value));
            case BOOL -> value != 0 ? "1" : "0";
            case ENUM -> {
                if (value < 0 || value >= fd.enumValues().size()) {
                    throw new IllegalArgumentException(
                        "Enum ordinal " + value + " out of range for field " + fd.name());
                }
                yield "'" + fd.enumValues().get((int) value).replace("'", "''") + "'";
            }
        };
    }
}
