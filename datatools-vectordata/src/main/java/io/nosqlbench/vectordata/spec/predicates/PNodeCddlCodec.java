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

import java.util.function.Function;

/// Renders a PNode predicate tree in CDDL-style notation.
///
/// ## Examples
///
/// ```
/// PredicateNode(fieldName="age", op=GT, comparands=[IntVal(42)])
///   →  "age: int > 42"
///
/// ConjugateNode(AND, [pred1, pred2])
///   →  "AND { pred1, pred2 }"
/// ```
public final class PNodeCddlCodec implements Function<PNode<?>, String> {

    /// Creates a new CDDL codec instance.
    public PNodeCddlCodec() {}

    @Override
    public String apply(PNode<?> node) {
        return render(node);
    }

    /// Renders the predicate tree in CDDL-style notation.
    /// @param node the predicate tree
    /// @return the CDDL string
    public static String render(PNode<?> node) {
        if (node instanceof ConjugateNode) {
            return renderConjugate((ConjugateNode) node);
        } else if (node instanceof PredicateNode) {
            return renderPredicate((PredicateNode) node);
        }
        throw new IllegalArgumentException("Unknown PNode type: " + node.getClass());
    }

    private static String renderConjugate(ConjugateNode cn) {
        StringBuilder sb = new StringBuilder();
        sb.append(cn.type().name()).append(" { ");
        for (int i = 0; i < cn.values().length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(render(cn.values()[i]));
        }
        sb.append(" }");
        return sb.toString();
    }

    private static String renderPredicate(PredicateNode pn) {
        StringBuilder sb = new StringBuilder();
        sb.append(fieldRef(pn));
        sb.append(": ");
        sb.append(comparandTypeName(pn));
        sb.append(' ').append(pn.op().symbol()).append(' ');
        appendComparands(sb, pn);
        return sb.toString();
    }

    private static String fieldRef(PredicateNode pn) {
        return pn.fieldName() != null ? pn.fieldName() : "F" + pn.field();
    }

    private static String comparandTypeName(PredicateNode pn) {
        if (pn.isTyped() && pn.comparands().length > 0) {
            Comparand first = pn.comparands()[0];
            if (first instanceof Comparand.IntVal) return "int";
            if (first instanceof Comparand.FloatVal) return "float";
            if (first instanceof Comparand.TextVal) return "tstr";
            if (first instanceof Comparand.BoolVal) return "bool";
            if (first instanceof Comparand.BytesVal) return "bstr";
            if (first instanceof Comparand.NullVal) return "null";
        }
        return "int";
    }

    private static void appendComparands(StringBuilder sb, PredicateNode pn) {
        if (pn.isTyped()) {
            if (pn.comparands().length == 1) {
                sb.append(renderComparand(pn.comparands()[0]));
            } else {
                sb.append('[');
                for (int i = 0; i < pn.comparands().length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(renderComparand(pn.comparands()[i]));
                }
                sb.append(']');
            }
        } else {
            if (pn.v().length == 1) {
                sb.append(pn.v()[0]);
            } else {
                sb.append('[');
                for (int i = 0; i < pn.v().length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(pn.v()[i]);
                }
                sb.append(']');
            }
        }
    }

    private static String renderComparand(Comparand c) {
        if (c instanceof Comparand.IntVal) return String.valueOf(((Comparand.IntVal) c).value());
        if (c instanceof Comparand.FloatVal) return String.valueOf(((Comparand.FloatVal) c).value());
        if (c instanceof Comparand.TextVal) return "\"" + ((Comparand.TextVal) c).value().replace("\"", "\\\"") + "\"";
        if (c instanceof Comparand.BoolVal) return ((Comparand.BoolVal) c).value() ? "true" : "false";
        if (c instanceof Comparand.BytesVal) return "h'...'";
        if (c instanceof Comparand.NullVal) return "null";
        return c.toString();
    }
}
