package ok.dht.test.kovalenko.http;

import ok.dht.Service;
import ok.dht.ServiceConfig;
import ok.dht.test.ServiceFactory;

import java.io.IOException;

@ServiceFactory(stage = 6, week = 1, bonuses = "SingleNodeTest#respectFileFolder")
public class MyServiceFactory implements ServiceFactory.Factory {

    @Override
    public Service create(ServiceConfig config) {
        try {
            return new MyServiceBase(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
