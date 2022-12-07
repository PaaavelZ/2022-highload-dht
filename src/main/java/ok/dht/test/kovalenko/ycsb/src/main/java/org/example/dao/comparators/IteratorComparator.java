package org.example.dao.comparators;

import org.example.dao.iterators.PeekIterator;
import org.example.dao.utils.DaoUtils;

import java.util.Comparator;

public final class IteratorComparator implements Comparator<PeekIterator> {

    public static final IteratorComparator INSTANSE = new IteratorComparator();

    private IteratorComparator() {
    }

    @Override
    public int compare(PeekIterator it1, PeekIterator it2) {
        if (it1.hasNext() && it2.hasNext()) {
            int compare = DaoUtils.entryComparator.compare(it1.peek(), it2.peek());
            if (compare == 0) {
                compare = Integer.compare(it1.getPriority(), it2.getPriority());
            }
            return compare;
        }
        // reverse compare
        return Boolean.compare(it2.hasNext(), it1.hasNext());
    }
}
