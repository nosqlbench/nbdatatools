package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

/// [T] is the node type, from
public sealed interface Node<T> extends BBWriter<T> permits ConjugateNode, PredicateNode {
}
