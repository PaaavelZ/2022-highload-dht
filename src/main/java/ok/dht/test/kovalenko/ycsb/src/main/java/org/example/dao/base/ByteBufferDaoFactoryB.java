package org.example.dao.base;

import org.example.ServiceConfig;
import org.example.dao.LSMDao;
import org.example.dao.aliases.TypedBaseTimedEntry;
import org.example.dao.aliases.TypedTimedEntry;
import org.example.dao.utils.DaoUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

@DaoFactoryB(stage = 5000, week = 1)
public final class ByteBufferDaoFactoryB implements DaoFactoryB.Factory<ByteBuffer, TypedTimedEntry> {

    public static final ByteBufferDaoFactoryB INSTANSE = new ByteBufferDaoFactoryB();

    private ByteBufferDaoFactoryB() {
    }

    @Override
    public Dao<ByteBuffer, TypedTimedEntry> createDao(ServiceConfig config) throws IOException {
        return new LSMDao(config);
    }

    @Override
    public String toString(ByteBuffer data) {
        ByteBuffer transfer = data.rewind().asReadOnlyBuffer();
        byte[] bytes = new byte[transfer.remaining()];
        transfer.get(bytes);
        return new String(bytes, DaoUtils.BASE_CHARSET);
    }

    @Override
    public ByteBuffer fromString(String data) {
        return data == null ? null : ByteBuffer.wrap(data.getBytes(DaoUtils.BASE_CHARSET));
    }

    @Override
    public TypedBaseTimedEntry fromBaseEntry(TimedEntry<ByteBuffer> baseTimedEntry) {
        return new TypedBaseTimedEntry(baseTimedEntry.timestamp(), baseTimedEntry.key(), baseTimedEntry.value());
    }

    @Override
    public byte[] toBytes(ByteBuffer bb) {
        byte[] bytes = new byte[bb.rewind().remaining()];
        bb.get(bytes);
        return bytes;
    }

}
