package org.example.utils;

import org.example.dao.LSMDao;
import org.example.dao.aliases.TypedBaseTimedEntry;
import org.example.dao.aliases.TypedTimedEntry;
import org.example.dao.utils.DaoUtils;
import org.example.http.LoadBalancer;
import org.example.http.MyServiceBase;
import org.example.http.Node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class DaoFiller {

    public static final DaoFiller INSTANSE = new DaoFiller();

    private DaoFiller() {
    }

    public static void fillDao(LSMDao dao, int idxEntryFrom, int idxEntryTo) throws InterruptedException, IOException {
        int sleepTreshold = 10_000;
        for (int i = idxEntryFrom; i <= idxEntryTo; ++i) {
            if (i % sleepTreshold == 0) {
                Thread.sleep(50);
            }
            dao.upsert(entryAt(i));
        }
        dao.flush();
    }

    public static void fillDaos(Map<String, MyServiceBase> urlsServices, int idxEntryFrom, int idxEntryTo)
            throws InterruptedException, IOException {
        int sleepTreshold = 10_000 * urlsServices.size();
        LoadBalancer loadBalancer = new LoadBalancer();
        Map<String, Node> urls = new HashMap<>();
        for (String url : urlsServices.keySet()) {
            urls.put(url, new Node(url));
        }
        for (int i = idxEntryFrom; i <= idxEntryTo; ++i) {
            if (i % sleepTreshold == 0) {
                Thread.sleep(100);
            }
            TypedTimedEntry entry = entryAt(i);
            Node responsibleNodeForKey = loadBalancer.responsibleNodeForKey(
                    DaoUtils.DAO_FACTORY.toString(entry.key()),
                    urls
            );
            urlsServices.get(responsibleNodeForKey.selfUrl()).getDao().upsert(entry);
        }
        for (Map.Entry<String, MyServiceBase> service : urlsServices.entrySet()) {
            service.getValue().getDao().flush();
        }
    }

    private static TypedBaseTimedEntry entryAt(int idx) {
        return new TypedBaseTimedEntry(
                System.currentTimeMillis(),
                DaoUtils.DAO_FACTORY.fromString(Integer.toString(idx)),
                DaoUtils.DAO_FACTORY.fromString(Integer.toString(idx))
        );
    }
}
