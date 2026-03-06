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

/// Renders a PNode predicate tree as a SQL WHERE clause fragment.
///
/// ## Examples
///
/// ```
/// PredicateNode(field=0, op=GT, v=[42])  →  "F0 > 42"
/// PredicateNode(fieldName="age", op=IN, comparands=[IntVal(1), IntVal(2)])  →  "age IN (1, 2)"
/// ConjugateNode(AND, [pred1, pred2])  →  "(pred1 AND pred2)"
/// ```
public final class PNodeSqlCodec implements Function<PNode<?>, String> {

    /// Creates a new SQL codec instance.
    public PNodeSqlCodec() {}

    @Override
    public String apply(PNode<?> node) {
        return render(node);
    }

    /// Renders the predicate tree as a SQL WHERE clause fragment.
    /// @param node the predicate tree
    /// @return the SQL string
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
        sb.append('(');
        for (int i = 0; i < cn.values().length; i++) {
            if (i > 0) sb.append(' ').append(op).append(' ');
            sb.append(render(cn.values()[i]));
        }
        sb.append(')');
        return sb.toString();
    }

    private static String renderPredicate(PredicateNode pn) {
        StringBuilder sb = new StringBuilder();
        sb.append(fieldRef(pn));
        sb.append(' ').append(sqlOp(pn.op()));

        if (pn.op() == OpType.IN) {
            sb.append(" (");
            appendComparands(sb, pn, ", ");
            sb.append(')');
        } else {
            sb.append(' ');
            appendFirstComparand(sb, pn);
        }
        return sb.toString();
    }

    private static String fieldRef(PredicateNode pn) {
        return pn.fieldName() != null ? pn.fieldName() : "F" + pn.field();
    }

    private static String sqlOp(OpType op) {
        switch (op) {
            case GT: return ">";
            case LT: return "<";
            case EQ: return "=";
            case NE: return "!=";
            case GE: return ">=";
            case LE: return "<=";
            case IN: return "IN";
            case MATCHES: return "LIKE";
            default: return op.symbol();
        }
    }

    private static void appendComparands(StringBuilder sb, PredicateNode pn, String delim) {
        if (pn.isTyped()) {
            for (int i = 0; i < pn.comparands().length; i++) {
                if (i > 0) sb.append(delim);
                sb.append(renderComparand(pn.comparands()[i]));
            }
        } else {
            for (int i = 0; i < pn.v().length; i++) {
                if (i > 0) sb.append(delim);
                sb.append(pn.v()[i]);
            }
        }
    }

    private static void appendFirstComparand(StringBuilder sb, PredicateNode pn) {
        if (pn.isTyped() && pn.comparands().length > 0) {
            sb.append(renderComparand(pn.comparands()[0]));
        } else if (pn.v().length > 0) {
            sb.append(pn.v()[0]);
        }
    }

    static String renderComparand(Comparand c) {
        if (c instanceof Comparand.IntVal) return String.valueOf(((Comparand.IntVal) c).value());
        if (c instanceof Comparand.FloatVal) return String.valueOf(((Comparand.FloatVal) c).value());
        if (c instanceof Comparand.TextVal) return "'" + ((Comparand.TextVal) c).value().replace("'", "''") + "'";
        if (c instanceof Comparand.BoolVal) return ((Comparand.BoolVal) c).value() ? "TRUE" : "FALSE";
        if (c instanceof Comparand.BytesVal) return "X'...'";
        if (c instanceof Comparand.NullVal) return "NULL";
        return c.toString();
    }
}
