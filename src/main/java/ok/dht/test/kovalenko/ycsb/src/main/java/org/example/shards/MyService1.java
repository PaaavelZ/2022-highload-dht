package org.example.shards;

import org.example.ServiceConfig;
import org.example.http.MyServiceBase;
import org.example.utils.Main;

import java.io.IOException;

public class MyService1 extends MyServiceBase {

    private static final int ORDINAL = 1;

    public MyService1(ServiceConfig config) throws IOException {
        super(config);
    }

    public static void main(String[] args) {
        Main.main(ORDINAL);
    }
}
