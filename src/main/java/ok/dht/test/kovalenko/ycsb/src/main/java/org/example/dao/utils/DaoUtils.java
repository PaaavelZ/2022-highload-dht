package org.example.dao.utils;

import org.example.dao.base.ByteBufferDaoFactoryB;
import org.example.dao.comparators.ByteBufferComparator;
import org.example.dao.comparators.EntryComparator;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class DaoUtils {

    public static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    public static final EntryComparator entryComparator = EntryComparator.INSTANSE;

    public static final ByteBufferComparator byteBufferComparator = ByteBufferComparator.INSTANSE;
    public static final int TIMESTAMP_LENGTH = Long.BYTES;
    public static final ByteBufferDaoFactoryB DAO_FACTORY = ByteBufferDaoFactoryB.INSTANSE;
    public static final Charset BASE_CHARSET = StandardCharsets.US_ASCII;

    private DaoUtils() {
    }
}
