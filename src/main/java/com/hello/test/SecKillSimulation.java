package com.hello.test;

import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SecKillSimulation {

    private volatile Map<String,Long> users = new ConcurrentHashMap<>();

    private AtomicInteger id = new AtomicInteger();



    public void createOrder(){
        int x  = id.addAndGet(1);
        String key = "user-"+x;
        users.put(key,System.currentTimeMillis());
        if(users.size() > 100){

        }
    }

    public void save(){

    }

    public static void main(String[] args) {

    }

}
