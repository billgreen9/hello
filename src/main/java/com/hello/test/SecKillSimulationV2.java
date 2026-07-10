package com.hello.test;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *
 * 最佳实践建议：在真实的秒杀中，通常是“入队即返回成功（前端提示排队中）”，然后通过WebSocket 推送或前端轮询来通知用户最终结果。如果业务方强求同步等待，请务必做好限流（Rate Limiting），保护数据库不被击穿。
 *
 */
public class SecKillSimulationV2 {

    // 1. 队列中存放的不再是纯 String，而是包含 Future 的任务对象
    private final BlockingQueue<OrderTask> orderQueue = new LinkedBlockingQueue<>(10000);
    private static final int BATCH_SIZE = 20;
    private static final long WAIT_TIME_MS = 100; // 批量等待时间
    // 业务线程等待落库的最大超时时间
    private static final long DB_TIMEOUT_MS = 2000;

    private final AtomicInteger id = new AtomicInteger();
    private final ExecutorService saveDBExecutor = Executors.newFixedThreadPool(4);
    private String path = null;

    // 封装订单任务，携带 CompletableFuture 以便唤醒业务线程
    private static class OrderTask {
        String orderKey;
        CompletableFuture<Boolean> future;

        OrderTask(String orderKey) {
            this.orderKey = orderKey;
            this.future = new CompletableFuture<>();
        }
    }

    public SecKillSimulationV2() {
        try {
            path = getClass().getResource("/").toURI().getPath();
            FileUtils.write(new File(path + "/run.log"), "");
            Thread consumer = new Thread(this::consumeLoop, "BatchConsumer");
            consumer.setDaemon(true);
            consumer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 2. 消费者核心循环
    private void consumeLoop() {
        List<OrderTask> batch = new ArrayList<>(BATCH_SIZE);
        while (true) {
            try {
                OrderTask task = orderQueue.poll(WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                if (task != null) {
                    batch.add(task);
                }

                boolean isBatchFull = batch.size() >= BATCH_SIZE;
                boolean isTimeoutAndHasData = batch.size() > 0 && task == null;

                if (isBatchFull || isTimeoutAndHasData) {
                    List<OrderTask> currentBatch = new ArrayList<>(batch);
                    // 提交给专属线程池异步落盘
                    saveDBExecutor.submit(() -> saveDB(currentBatch));
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // 3. 秒杀接口：入队后，阻塞等待数据库落库结果.但是
    // 最佳实践建议：在真实的秒杀中，通常是“入队即返回成功（前端提示排队中）”，然后通过WebSocket 推送或前端轮询来通知用户最终结果。如果业务方强求同步等待，请务必做好限流（Rate Limiting），保护数据库不被击穿。
    public boolean createOrder() {
        long s1 = System.currentTimeMillis();
        int x = id.addAndGet(1);
        String key = "order-" + String.format("%03d", x);

        OrderTask task = new OrderTask(key);
        boolean queued = orderQueue.offer(task);
        if (!queued) {
            System.err.println("队列已满，订单 " + key + " 被拒绝！");
            return false;
        }

        // 核心：业务线程在此阻塞，等待消费者线程落库完成
        /// ///
        try {
            Boolean result = task.future.get(DB_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            long s2 = System.currentTimeMillis();
            System.out.println("id=" + x + " 耗时=" + (s2 - s1) + "ms, 落库结果=" + result);
            return result;
        } catch (TimeoutException e) {
            // 等待超时，说明数据库处理太慢
            System.err.println("订单 " + key + " 等待落库超时！");
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    // 4. 批量落盘，落盘成功后唤醒所有相关的业务线程
    private void saveDB(List<OrderTask> tasks) {
        System.out.println("[批量入库] size=" + tasks.size());
        try {
            Thread.sleep(5); // 模拟 DB 耗时
            List<String> keys = new ArrayList<>();
            for (OrderTask task : tasks) {
                keys.add(task.orderKey);
            }
            String logFile = path + "/run.log";
            FileUtils.writeLines(new File(logFile), keys, true);

            // 落库成功，唤醒所有正在等待的业务线程
            for (OrderTask task : tasks) {
                task.future.complete(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 落库失败，也要唤醒业务线程，告知其失败
            for (OrderTask task : tasks) {
                task.future.complete(false);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        SecKillSimulationV2 simulation = new SecKillSimulationV2();
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                for (int j = 0; j < 50; j++) {
                    simulation.createOrder();
                }
            }).start();
        }
        Thread.sleep(5000);
        System.exit(0);
    }
}