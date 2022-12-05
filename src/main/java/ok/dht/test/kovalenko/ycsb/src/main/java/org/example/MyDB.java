package org.example;

import ok.dht.ServiceConfig;
import ok.dht.test.kovalenko.dao.LSMDao;
import ok.dht.test.kovalenko.dao.aliases.TypedBaseTimedEntry;
import ok.dht.test.kovalenko.dao.aliases.TypedTimedEntry;
import ok.dht.test.kovalenko.dao.utils.DaoUtils;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class MyDB extends DB {

    private LSMDao dao;

    @Override
    public void init() throws DBException {
        try {
            Properties p = getProperties(); // As expected, from .properties or a similar file
            int selfPort = (int) p.get("selfPort");
            String selfUrl = (String) p.get("selfUrl");
            List<String> clusterUrls = List.of((String[]) p.get("clusterUrls"));
            Path workingDir = Path.of((String) p.get("workingDir"));

            dao = new LSMDao(
                    new ServiceConfig(
                            selfPort,
                            selfUrl,
                            clusterUrls,
                            workingDir
                    )
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void cleanup() throws DBException {
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            // As expected, 'table' and 'fields' don't matter
            ByteBuffer bbKey = DaoUtils.DAO_FACTORY.fromString(key);
            var res = dao.get(bbKey);
            if (res.value() == null) {
                return Status.NOT_FOUND;
            }
            String value = DaoUtils.DAO_FACTORY.toString(res.value());
            var byteIteratorMap = StringByteIterator.getByteIteratorMap(Collections.singletonMap(key, value));
            result.put(key, byteIteratorMap.get(key));

            return Status.OK;
        } catch (IOException e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            // As expected, 'table' doesn't matter
            ByteBuffer bbKey = DaoUtils.DAO_FACTORY.fromString(key);
            ByteIterator valueIt = values.values().iterator().next();
            ByteBuffer bbValue = null;
            if (valueIt != null) {
                bbValue = DaoUtils.DAO_FACTORY.fromString(valueIt.toString());
            }
            TypedTimedEntry entry = new TypedBaseTimedEntry(System.currentTimeMillis(), bbKey, bbValue);
            dao.upsert(entry);
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String table, String key) {
        return insert(table, key, Collections.singletonMap(key, null));
    }

}
