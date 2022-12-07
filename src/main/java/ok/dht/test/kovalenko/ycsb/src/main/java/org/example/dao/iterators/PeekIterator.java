package org.example.dao.iterators;

import org.example.dao.aliases.TypedIterator;
import org.example.dao.aliases.TypedTimedEntry;

import java.util.Iterator;

public class PeekIterator implements TypedIterator {

    private final int priority;
    private final Iterator<TypedTimedEntry> delegate;
    private TypedTimedEntry peek;

    public PeekIterator(Iterator<TypedTimedEntry> delegate, int priority) {
        this.delegate = delegate;
        this.priority = priority;
    }

    @Override
    public boolean hasNext() {
        return peek != null || delegate.hasNext();
    }

    @Override
    public TypedTimedEntry next() {
        TypedTimedEntry peekNext = peek();
        this.peek = null;
        return peekNext;
    }

    public TypedTimedEntry peek() {
        if (peek == null && delegate.hasNext()) {
            peek = delegate.next();
        }
        return peek;
    }

    public int getPriority() {
        return priority;
    }
}
