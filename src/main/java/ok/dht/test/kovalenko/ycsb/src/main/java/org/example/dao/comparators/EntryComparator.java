package org.example.dao.comparators;

import org.example.dao.aliases.TypedTimedEntry;
import org.example.dao.utils.DaoUtils;

import java.util.Comparator;

public final class EntryComparator
        implements Comparator<TypedTimedEntry> {

    public static final EntryComparator INSTANSE = new EntryComparator();

    private EntryComparator() {
    }

    @Override
    public int compare(TypedTimedEntry e1, TypedTimedEntry e2) {
        return DaoUtils.byteBufferComparator.compare(e1.key(), e2.key());
    }
}
