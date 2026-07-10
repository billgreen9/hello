package com.hello.test;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Deprecated
public class SecKillSimulation {

    private volatile Map<String,Order> orders = new ConcurrentHashMap<>();

    private static int wait_time = 100;

    private Runnable consumer = () -> {
        int internal = wait_time;
        long lastTime = System.currentTimeMillis();
        while (true){
            long now = System.currentTimeMillis();
            if(orders.size() > batchSize || now - lastTime >= internal){
                lastTime = now;
                Map<String,Order> olders = orders;
                orders = new ConcurrentHashMap<>();
                CompletableFuture.runAsync(() -> {
                    if(olders.size() == 0){
                        return ;
                    }
                    saveDB(olders);
                });
            }else{
                try {
                    Thread.sleep(internal);
                }catch (Exception e){
                }
            }

        }
    };

    private static int batchSize = 20;


    private AtomicInteger id = new AtomicInteger();
    private String path = null;

    public SecKillSimulation() {
        try {
            path = getClass().getResource("/").toURI().getPath();
            String logFile = path + "/run.log";
            System.out.println("path="+path);
            FileUtils.write(new File(logFile),"");
            Thread t = new Thread(consumer);
            t.setName("consumerScanner");
            t.start();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static class Order{
        private String id;
        private long createTime;
        private volatile boolean ready = false;

        public Order(String id){
            this.id = id;
            this.createTime = System.currentTimeMillis();
        }

    }


    private void checkCondition(){

    }

    public void createOrder(){
        long s1 = System.currentTimeMillis();
        checkCondition();
        int x  = id.addAndGet(1);
        String key = "order-"+String.format("%03d",x);
        Order order = new Order(key);
        orders.put(key,order);

        synchronized (order){
            while (!order.ready){
                try {
                    order.wait(wait_time*2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
           save(order);
        }

        long s2 = System.currentTimeMillis();
        System.out.println("id="+x+" createOrder cost time="+(s2-s1));
    }

    private void save(Order tmp){
        if(tmp.ready){
            return ;
        }
        tmp.ready = true;
        synchronized (tmp){
            tmp.notifyAll();
        }
    }

    public int saveDB(Map<String,Order> olders){
        int size = olders.size();
        System.out.println("size="+size);
        try {
            Thread.sleep(5);
        }catch (InterruptedException e){
            new RuntimeException(e);
        }
        for(String order:olders.keySet()){
          save(olders.get(order));
        }
        logs(olders.keySet().stream().map(order->order).collect(Collectors.toList()));
        return size;

       }

    private int logs(List<String> lines) {
        try {
           // System.out.println("logs size="+lines.size());
            String logFile = path + "/run.log";
            FileUtils.writeLines(new File(logFile), lines, true);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        return lines.size();
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
            for(int i=0;i<50;i++) {
                simulation.createOrder();
            }
        };


       // ExecutorService pool = Executors.newCachedThreadPool();
        for(int i=0;i<20;i++){
            new Thread(runnable).start();
        }
    }

}
