package org.example.dao;

import org.example.ServiceConfig;
import org.example.dao.aliases.DiskSSTable;
import org.example.dao.aliases.MappedFileDiskSSTableStorage;
import org.example.dao.aliases.MemorySSTable;
import org.example.dao.aliases.MemorySSTableStorage;
import org.example.dao.aliases.TypedTimedEntry;
import org.example.dao.base.Dao;
import org.example.dao.iterators.MergeIterator;
import org.example.dao.runnables.CompactRunnable;
import org.example.dao.runnables.FlushRunnable;
import org.example.dao.utils.DaoUtils;
import org.example.dao.utils.PoolKeeper;
import org.example.dao.visitors.ConfigVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LSMDao implements Dao<ByteBuffer, TypedTimedEntry> {

    private static final int N_MEMORY_SSTABLES = 2;
    private static final int FLUSH_TRESHOLD_BYTES = 10 * (1 << 20); // 10MB
    private final ServiceConfig config;
    private final Serializer serializer;
    private final AtomicLong filesCounter = new AtomicLong();

    private final MemoryStorage memoryStorage = new MemoryStorage(N_MEMORY_SSTABLES);
    private final MappedFileDiskSSTableStorage diskStorage;
    private final AtomicBoolean wasCompacted = new AtomicBoolean(true);
    private static final int CORE_POOL_SIZE = 1;
    private static final int MAX_POOL_SIZE = 4 * (Runtime.getRuntime().availableProcessors() + 1);
    private static final int QUEUE_CAPACITY = 10 * MAX_POOL_SIZE;
    private final PoolKeeper poolKeeper = new PoolKeeper(CORE_POOL_SIZE, MAX_POOL_SIZE, QUEUE_CAPACITY, "DaoThread");
    private final Runnable flushRunnable;
    private final Runnable compactRunnable;
    private final AtomicLong curBytesForEntries = new AtomicLong();

    public LSMDao(ServiceConfig config) throws IOException {
        try {
            this.config = config;
            this.serializer = new Serializer(this.wasCompacted);
            if (Files.exists(config.workingDir())) {
                Files.walkFileTree(
                        config.workingDir(),
                        new ConfigVisitor(this.config, this.serializer, this.filesCounter)
                );
            } else {
                Files.createDirectory(config.workingDir());
            }
            this.diskStorage = new MappedFileDiskSSTableStorage(config, this.serializer, this.filesCounter);
            this.flushRunnable
                    = new FlushRunnable(this.config, this.serializer, this.memoryStorage, this.filesCounter);
            this.compactRunnable
                    = new CompactRunnable(this.config, this.serializer, this.diskStorage, this.wasCompacted,
                    this.filesCounter);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public TypedTimedEntry get(ByteBuffer key) throws IOException {
        try {
            TypedTimedEntry res = this.memoryStorage.get(key);
            if (res == null) {
                res = this.diskStorage.get(key);
            }
            return res; // including tombstone
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Iterator<TypedTimedEntry> get(ByteBuffer from, ByteBuffer to) throws IOException {
        try {
            List<Iterator<TypedTimedEntry>> memIts = this.memoryStorage.get(from, to);
            List<Iterator<TypedTimedEntry>> diskIts = this.diskStorage.get(from, to);
            return new MergeIterator(memIts, diskIts);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void upsert(TypedTimedEntry entry) {
        try {
            if (this.curBytesForEntries.get() >= FLUSH_TRESHOLD_BYTES) {
                this.flush();
            }

            if (this.memoryStorage.writeSSTables().isEmpty()) {
                throw new RuntimeException("Very large number of upserting");
            }

            this.memoryStorage.writeSSTables().peek().put(entry.key(), entry);
            this.curBytesForEntries.addAndGet(DiskSSTable.sizeOf(entry));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        if (this.memoryStorage.writeSSTables().isEmpty() || this.memoryStorage.writeSSTables().peek().isEmpty()) {
            return;
        }
        this.poolKeeper.submit(this.flushRunnable);
        this.curBytesForEntries.set(0);
    }

    @Override
    public synchronized void close() throws IOException {
        this.flush();
        this.poolKeeper.close();
        this.memoryStorage.clear();
        this.diskStorage.close();
    }

    @Override
    public synchronized void compact() {
        if (this.wasCompacted.get()) {
            return;
        }
        this.poolKeeper.submit(this.compactRunnable);
    }

    public static final class MemoryStorage {

        private final MemorySSTableStorage memoryWriteSSTables;
        private final MemorySSTableStorage memoryFlushSSTables;

        public MemoryStorage(int size) {
            this.memoryWriteSSTables = new MemorySSTableStorage(size);
            this.memoryFlushSSTables = new MemorySSTableStorage(size);
            for (int i = 0; i < size; ++i) {
                this.memoryWriteSSTables.add(new MemorySSTable());
            }
        }

        public MemorySSTableStorage writeSSTables() {
            return this.memoryWriteSSTables;
        }

        public MemorySSTableStorage flushSSTables() {
            return this.memoryFlushSSTables;
        }

        public TypedTimedEntry get(ByteBuffer key) {
            TypedTimedEntry res = this.memoryWriteSSTables.get(key);
            if (res == null) {
                res = this.memoryFlushSSTables.get(key);
            }
            return res;
        }

        public List<Iterator<TypedTimedEntry>> get(ByteBuffer from, ByteBuffer to) {
            List<Iterator<TypedTimedEntry>> res = new ArrayList<>();
            ByteBuffer from1 = from == null ? DaoUtils.EMPTY_BYTEBUFFER : from;
            addMemorySSTables(this.memoryWriteSSTables.iterator(), res, from1, to);
            addMemorySSTables(this.memoryFlushSSTables.iterator(), res, from1, to);
            return res;
        }

        public int size() {
            return writeSSTables().size() + flushSSTables().size();
        }

        public boolean empty() {
            return !this.memoryWriteSSTables.isEmpty() && this.memoryWriteSSTables.peek().isEmpty()
                    && this.memoryFlushSSTables.isEmpty();
        }

        public void clear() {
            this.memoryWriteSSTables.clear();
            this.memoryFlushSSTables.clear();
        }

        private void addMemorySSTables(Iterator<MemorySSTable> it,
                                       List<Iterator<TypedTimedEntry>> iterators,
                                       ByteBuffer from, ByteBuffer to) {
            while (it.hasNext()) {
                MemorySSTable memorySSTable = it.next();
                if (memorySSTable.isEmpty()) {
                    continue;
                }
                Iterator<TypedTimedEntry> rangeIt = memorySSTable.get(from, to);
                if (rangeIt == null) {
                    continue;
                }
                iterators.add(rangeIt);
            }
        }
    }
}
