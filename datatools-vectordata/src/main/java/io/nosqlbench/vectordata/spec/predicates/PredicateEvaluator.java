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

import java.util.regex.Pattern;

/// Evaluates a predicate tree against an MNode record.
///
/// ## Field resolution
///
/// - Named predicates look up fields by key in the MNode
/// - Indexed predicates use the positional index into MNode's parallel arrays
///
/// ## Comparison semantics
///
/// Values are compared using numeric promotion: integer fields are compared
/// as {@code long}, float fields as {@code double}. String fields support
/// EQ, NE, IN, and MATCHES (regex). Boolean fields support EQ and NE.
public final class PredicateEvaluator {

    private PredicateEvaluator() {}

    /// Evaluates a predicate tree against an MNode record.
    ///
    /// @param predicate the predicate tree to evaluate
    /// @param record    the MNode record to test against
    /// @return true if the record matches the predicate
    public static boolean matches(PNode<?> predicate, MNode record) {
        if (predicate instanceof ConjugateNode) {
            return evaluateConjugate((ConjugateNode) predicate, record);
        } else if (predicate instanceof PredicateNode) {
            return evaluatePredicate((PredicateNode) predicate, record);
        }
        throw new IllegalArgumentException("Unknown PNode type: " + predicate.getClass());
    }

    private static boolean evaluateConjugate(ConjugateNode cn, MNode record) {
        switch (cn.type()) {
            case AND: {
                for (PNode<?> child : cn.values()) {
                    if (!matches(child, record)) return false;
                }
                return true;
            }
            case OR: {
                for (PNode<?> child : cn.values()) {
                    if (matches(child, record)) return true;
                }
                return false;
            }
            default:
                throw new IllegalStateException("PRED is not a valid conjugate type");
        }
    }

    private static boolean evaluatePredicate(PredicateNode pn, MNode record) {
        Object fieldValue;
        if (pn.fieldName() != null) {
            fieldValue = record.get(pn.fieldName());
        } else {
            throw new IllegalArgumentException(
                "PredicateEvaluator requires named predicates (fieldName != null). " +
                "Use PredicateContext to resolve indexed predicates to named predicates.");
        }
        if (fieldValue == null) {
            return pn.op() == OpType.EQ && hasNullComparand(pn);
        }

        if (pn.isTyped()) {
            return evaluateTyped(pn.op(), fieldValue, pn.comparands());
        }
        return evaluateLong(pn.op(), fieldValue, pn.v());
    }

    private static boolean hasNullComparand(PredicateNode pn) {
        if (pn.isTyped()) {
            for (Comparand c : pn.comparands()) {
                if (c instanceof Comparand.NullVal) return true;
            }
        }
        return false;
    }

    private static boolean evaluateLong(OpType op, Object fieldValue, long[] comparands) {
        if (fieldValue instanceof Number) {
            long fv = ((Number) fieldValue).longValue();
            switch (op) {
                case GT: return comparands.length > 0 && fv > comparands[0];
                case LT: return comparands.length > 0 && fv < comparands[0];
                case EQ: return comparands.length > 0 && fv == comparands[0];
                case NE: return comparands.length > 0 && fv != comparands[0];
                case GE: return comparands.length > 0 && fv >= comparands[0];
                case LE: return comparands.length > 0 && fv <= comparands[0];
                case IN: {
                    for (long c : comparands) {
                        if (fv == c) return true;
                    }
                    return false;
                }
                default: return false;
            }
        }
        if (fieldValue instanceof Boolean) {
            long bvl = ((Boolean) fieldValue) ? 1 : 0;
            switch (op) {
                case EQ: return comparands.length > 0 && bvl == comparands[0];
                case NE: return comparands.length > 0 && bvl != comparands[0];
                default: return false;
            }
        }
        return false;
    }

    private static boolean evaluateTyped(OpType op, Object fieldValue, Comparand[] comparands) {
        if (comparands.length == 0) return false;

        if (fieldValue instanceof Number) {
            return evaluateNumericTyped(op, (Number) fieldValue, comparands);
        }
        if (fieldValue instanceof String) {
            return evaluateStringTyped(op, (String) fieldValue, comparands);
        }
        if (fieldValue instanceof Boolean) {
            return evaluateBoolTyped(op, (Boolean) fieldValue, comparands);
        }
        return false;
    }

    private static boolean evaluateNumericTyped(OpType op, Number fieldVal, Comparand[] comparands) {
        Comparand first = comparands[0];
        if (first instanceof Comparand.IntVal) {
            long cv = ((Comparand.IntVal) first).value();
            long fv = fieldVal.longValue();
            switch (op) {
                case GT: return fv > cv;
                case LT: return fv < cv;
                case EQ: return fv == cv;
                case NE: return fv != cv;
                case GE: return fv >= cv;
                case LE: return fv <= cv;
                case IN: {
                    for (Comparand c : comparands) {
                        if (c instanceof Comparand.IntVal && fv == ((Comparand.IntVal) c).value()) return true;
                    }
                    return false;
                }
                default: return false;
            }
        }
        if (first instanceof Comparand.FloatVal) {
            double cv = ((Comparand.FloatVal) first).value();
            double fv = fieldVal.doubleValue();
            switch (op) {
                case GT: return fv > cv;
                case LT: return fv < cv;
                case EQ: return fv == cv;
                case NE: return fv != cv;
                case GE: return fv >= cv;
                case LE: return fv <= cv;
                case IN: {
                    for (Comparand c : comparands) {
                        if (c instanceof Comparand.FloatVal && fv == ((Comparand.FloatVal) c).value()) return true;
                    }
                    return false;
                }
                default: return false;
            }
        }
        return false;
    }

    private static boolean evaluateStringTyped(OpType op, String fieldVal, Comparand[] comparands) {
        Comparand first = comparands[0];
        if (!(first instanceof Comparand.TextVal)) return false;
        String cv = ((Comparand.TextVal) first).value();
        switch (op) {
            case GT: return fieldVal.compareTo(cv) > 0;
            case LT: return fieldVal.compareTo(cv) < 0;
            case EQ: return fieldVal.equals(cv);
            case NE: return !fieldVal.equals(cv);
            case GE: return fieldVal.compareTo(cv) >= 0;
            case LE: return fieldVal.compareTo(cv) <= 0;
            case IN: {
                for (Comparand c : comparands) {
                    if (c instanceof Comparand.TextVal && fieldVal.equals(((Comparand.TextVal) c).value()))
                        return true;
                }
                return false;
            }
            case MATCHES: return Pattern.matches(cv, fieldVal);
            default: return false;
        }
    }

    private static boolean evaluateBoolTyped(OpType op, Boolean fieldVal, Comparand[] comparands) {
        Comparand first = comparands[0];
        if (!(first instanceof Comparand.BoolVal)) return false;
        boolean cv = ((Comparand.BoolVal) first).value();
        switch (op) {
            case EQ: return fieldVal == cv;
            case NE: return fieldVal != cv;
            default: return false;
        }
    }
}
