package com.hello.test;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SecKillSimulation {

    private volatile Map<String,Long> orders = new ConcurrentHashMap<>();

    private ExecutorService saveDBExecutor = Executors.newFixedThreadPool(10);

    private int batchSize = 20;

    private AtomicInteger id = new AtomicInteger();
    private String path = null;

    public SecKillSimulation() {
        try {
            path = getClass().getResource("/").toURI().getPath();
            String logFile = path + "/run.log";
            System.out.println("path="+path);
            FileUtils.write(new File(logFile),"");
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }


    private void checkCondition(){

    }

    public void createOrder(){
        checkCondition();
        int x  = id.addAndGet(1);
        String key = "order-"+String.format("%03d",x);
        orders.put(key,System.currentTimeMillis());

        if(orders.size() >= batchSize){
            synchronized (this){
                if(orders.size() >= batchSize){
                    this.notifyAll();
                }
            }
        }else{
            synchronized (this){
                try {
                    this.wait(100L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                save();
            }
        }
    }

    public void save(){
        Map<String,Long> tmp = new ConcurrentHashMap<>();
        Map<String,Long> olders = orders;
        orders = tmp;
        try {
            Thread.sleep(new Random().nextInt(1000));
        }catch (InterruptedException e){
            new RuntimeException(e);
        }

        logs(olders.keySet().stream().map(order->order).collect(Collectors.toList()));

       }

    private void logs(List<String> lines) {
        try {
            String logFile = path + "/run.log";
            FileUtils.writeLines(new File(logFile), lines, true);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void log(String order) {
        try {
            String logFile = path + "/run.log";
            FileUtils.write(new File(logFile), order, Charset.forName("UTF-8"), true);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        SecKillSimulation simulation = new SecKillSimulation();
        Runnable runnable = () -> {
            simulation.createOrder();
        };


       // ExecutorService pool = Executors.newCachedThreadPool();
        for(int i=0;i<300;i++){
            new Thread(runnable).start();
        }
    }

}
