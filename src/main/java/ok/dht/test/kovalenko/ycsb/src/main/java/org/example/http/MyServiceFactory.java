package org.example.http;

import org.example.Service;
import org.example.ServiceConfig;
import org.example.ServiceFactory;

import java.io.IOException;

@ServiceFactory(stage = 7, week = 1, bonuses = "SingleNodeTest#respectFileFolder")
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
