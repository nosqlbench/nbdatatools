package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

import java.nio.ByteBuffer;

public sealed interface Node<T> extends BBWriter<T> permits ConjugateNode, PredicateNode {
    static Node<?> fromBuffer(ByteBuffer b) {
        byte typeOrdinal = b.get(b.position());
        return switch(ConjugateType.values()[typeOrdinal]) {
            case AND, OR -> new ConjugateNode(b);
            case PRED -> new PredicateNode(b);
        };
    }
}
