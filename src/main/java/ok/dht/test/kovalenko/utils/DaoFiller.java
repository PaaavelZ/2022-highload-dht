package ok.dht.test.kovalenko.utils;

import ok.dht.test.kovalenko.http.LoadBalancer;
import ok.dht.test.kovalenko.http.MyServiceBase;
import ok.dht.test.kovalenko.http.Node;
import ok.dht.test.kovalenko.dao.LSMDao;
import ok.dht.test.kovalenko.dao.aliases.TypedBaseTimedEntry;
import ok.dht.test.kovalenko.dao.aliases.TypedTimedEntry;
import ok.dht.test.kovalenko.dao.utils.DaoUtils;

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
