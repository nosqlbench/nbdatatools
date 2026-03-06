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

/// Renders a PNode predicate tree as a CQL WHERE clause fragment.
///
/// CQL differs from SQL in minor ways:
/// - No parenthesized AND/OR grouping (CQL only supports AND at top level)
/// - MATCHES is not a CQL keyword; rendered as a comment
///
/// ## Examples
///
/// ```
/// PredicateNode(fieldName="age", op=GT, v=[42])  →  "age > 42"
/// ConjugateNode(AND, [pred1, pred2])  →  "pred1 AND pred2"
/// ```
public final class PNodeCqlCodec implements Function<PNode<?>, String> {

    /// Creates a new CQL codec instance.
    public PNodeCqlCodec() {}

    @Override
    public String apply(PNode<?> node) {
        return render(node);
    }

    /// Renders the predicate tree as a CQL WHERE clause fragment.
    /// @param node the predicate tree
    /// @return the CQL string
    public static String render(PNode<?> node) {
        if (node instanceof ConjugateNode) {
            return renderConjugate((ConjugateNode) node);
        } else if (node instanceof PredicateNode) {
            return renderPredicate((PredicateNode) node);
        }
        throw new IllegalArgumentException("Unknown PNode type: " + node.getClass());
    }

    private static String renderConjugate(ConjugateNode cn) {
        String op = cn.type().name();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cn.values().length; i++) {
            if (i > 0) sb.append(' ').append(op).append(' ');
            sb.append(render(cn.values()[i]));
        }
        return sb.toString();
    }

    private static String renderPredicate(PredicateNode pn) {
        StringBuilder sb = new StringBuilder();
        sb.append(fieldRef(pn));

        if (pn.op() == OpType.IN) {
            sb.append(" IN (");
            appendComparands(sb, pn, ", ");
            sb.append(')');
        } else if (pn.op() == OpType.MATCHES) {
            sb.append(" /* MATCHES */ '");
            if (pn.isTyped() && pn.comparands().length > 0
                && pn.comparands()[0] instanceof Comparand.TextVal) {
                sb.append(((Comparand.TextVal) pn.comparands()[0]).value());
            } else if (pn.v().length > 0) {
                sb.append(pn.v()[0]);
            }
            sb.append('\'');
        } else {
            sb.append(' ').append(pn.op().symbol()).append(' ');
            if (pn.isTyped() && pn.comparands().length > 0) {
                sb.append(PNodeSqlCodec.renderComparand(pn.comparands()[0]));
            } else if (pn.v().length > 0) {
                sb.append(pn.v()[0]);
            }
        }
        return sb.toString();
    }

    private static String fieldRef(PredicateNode pn) {
        return pn.fieldName() != null ? pn.fieldName() : "F" + pn.field();
    }

    private static void appendComparands(StringBuilder sb, PredicateNode pn, String delim) {
        if (pn.isTyped()) {
            for (int i = 0; i < pn.comparands().length; i++) {
                if (i > 0) sb.append(delim);
                sb.append(PNodeSqlCodec.renderComparand(pn.comparands()[i]));
            }
        } else {
            for (int i = 0; i < pn.v().length; i++) {
                if (i > 0) sb.append(delim);
                sb.append(pn.v()[i]);
            }
        }
    }
}
