package org.example;

import org.example.dao.utils.DaoUtils;
import org.example.http.Client;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

public class MyDB extends DB {

    private String workingUrl;

    @Override
    public void init() throws DBException {
        // Example url: http://localhost:19234
        workingUrl = (String) getProperties().get("workingUrl");
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            // As expected, 'table' and 'fields' don't matter
            var cf = Client.INSTANSE.get(uriForKey(key));
            var response = cf.get(); // No async interaction cause of a contract for 'return StatusCode'

            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                return Status.SERVICE_UNAVAILABLE;
            }

            if (response.body().length == 0) {
                return Status.NOT_FOUND;
            }

            String value = new String(response.body(), DaoUtils.BASE_CHARSET);
            var byteIteratorMap = StringByteIterator.getByteIteratorMap(Collections.singletonMap(key, value));
            result.put(key, byteIteratorMap.get(key));
            return Status.OK;
        } catch (ExecutionException | InterruptedException e) {
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
            ByteIterator valueIt = values.values().iterator().next();
            byte[] value = valueIt.toString().getBytes(DaoUtils.BASE_CHARSET);
            var cf = Client.INSTANSE.put(uriForKey(key), value);
            var response = cf.get(); // No async interaction cause of a contract for 'return StatusCode'

            if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
                return Status.SERVICE_UNAVAILABLE;
            }

            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String table, String key) {
        try {
            // As expected, 'table' doesn't matter
            var cf = Client.INSTANSE.delete(uriForKey(key));
            var response = cf.get(); // No async interaction cause of a contract for 'return StatusCode'

            if (response.statusCode() != HttpURLConnection.HTTP_ACCEPTED) {
                return Status.SERVICE_UNAVAILABLE;
            }

            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    private String uriForKey(String key) {
        return workingUrl + "/v0/entity?id=" + key + "&ack=2&from=3";
    }

}
