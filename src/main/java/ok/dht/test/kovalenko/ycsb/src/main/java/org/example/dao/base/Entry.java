package org.example.dao.base;

public interface Entry<D> {
    D key();

    D value();

    default boolean isTombstone() {
        return value() == null;
    }
}
