package org.example.dao.iterators;

import org.example.dao.aliases.TypedIterator;
import org.example.dao.aliases.TypedTimedEntry;
import org.example.dao.comparators.IteratorComparator;
import org.example.dao.utils.MergeIteratorUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator
        implements TypedIterator {

    private final Queue<PeekIterator> iterators = new PriorityQueue<>(IteratorComparator.INSTANSE);
    private final TombstoneFilteringIterator tombstoneFilteringIterator;

    public MergeIterator(List<Iterator<TypedTimedEntry>> memoryIterators, List<Iterator<TypedTimedEntry>> diskIterators)
            throws IOException, ReflectiveOperationException {
        memoryIterators.forEach(it -> iterators.add(new PeekIterator(it, iterators.size())));
        diskIterators.forEach(it -> iterators.add(new PeekIterator(it, iterators.size())));
        this.tombstoneFilteringIterator = new TombstoneFilteringIterator(iterators);
    }

    @Override
    public boolean hasNext() {
        return tombstoneFilteringIterator.hasNext();
    }

    @Override
    public TypedTimedEntry next() {
        TypedTimedEntry result = tombstoneFilteringIterator.next();
        MergeIteratorUtils.skipEntry(iterators, result);
        return result;
    }
}
