package com.aliyun.odps.jdbc.performance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;

public class BigSQLPerformanceTest {

    private static final int CONCURRENT_THREADS = 20;
    private static final int TEST_ROUNDS = 100;

    @Test
    public void testConcurrentBigSQL() throws Exception {
        String sql = readBigSQL();
        
        System.out.println("=== BigSQL 性能测试 ===");
        System.out.println("SQL 长度: " + sql.length() + " 字符");
        System.out.println("并发线程数: " + CONCURRENT_THREADS);
        System.out.println("测试轮数: " + TEST_ROUNDS);
        System.out.println();
        
        List<Long> executionTimes = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(TEST_ROUNDS);
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < TEST_ROUNDS; i++) {
            final int round = i + 1;
            executor.submit(() -> {
                try {
                    long roundTime = executeSQL(sql);
                    synchronized (executionTimes) {
                        executionTimes.add(roundTime);
                    }
                    System.out.println("第 " + round + " 轮完成, 耗时: " + roundTime + " ms");
                } catch (Exception e) {
                    System.err.println("第 " + round + " 轮执行失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        
        long totalTime = System.currentTimeMillis() - startTime;
        
        printStatistics(executionTimes, totalTime);

        if (totalTime > 300_000) {
            throw new RuntimeException("TOO SLOW! Please check if there performance issue. cost time : " + totalTime + "ms");
        }
    }

    private long executeSQL(String sql) throws Exception {
//        ImmutableMap<String, String> map =
//        ImmutableMap.of("skipCheckIfSelect", "false");
//        Connection conn = TestUtils.getConnection(map);

        Connection conn = TestUtils.getConnection();
        Statement stmt = conn.createStatement();
        
        long startTime = System.currentTimeMillis();
        
        try {
          ResultSet rs = stmt.executeQuery(sql);

          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
          }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();


        stmt.close();
        conn.close();
        
        return endTime - startTime;
    }

    private String readBigSQL() throws IOException {
        String path = getClass().getClassLoader().getResource("performance/big_sql.txt").getPath();
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    private void printStatistics(List<Long> executionTimes, long totalTime) {
        if (executionTimes.isEmpty()) {
            System.out.println("没有成功的执行记录");
            return;
        }

        long min = executionTimes.stream().min(Long::compare).orElse(0L);
        long max = executionTimes.stream().max(Long::compare).orElse(0L);
        double avg = executionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long sum = executionTimes.stream().mapToLong(Long::longValue).sum();
        
        List<Long> sorted = new ArrayList<>(executionTimes);
        sorted.sort(Long::compare);
        long p50 = sorted.get((int) (sorted.size() * 0.5));
        long p90 = sorted.get((int) (sorted.size() * 0.9));
        long p95 = sorted.get((int) (sorted.size() * 0.95));
        long p99 = sorted.get((int) (sorted.size() * 0.99));

        System.out.println("\n=== 性能统计 ===");
        System.out.println("总执行时间: " + totalTime + " ms");
        System.out.println("成功执行次数: " + executionTimes.size());
        System.out.println("最小耗时: " + min + " ms");
        System.out.println("最大耗时: " + max + " ms");
        System.out.println("平均耗时: " + String.format("%.2f", avg) + " ms");
        System.out.println("P50 耗时: " + p50 + " ms");
        System.out.println("P90 耗时: " + p90 + " ms");
        System.out.println("P95 耗时: " + p95 + " ms");
        System.out.println("P99 耗时: " + p99 + " ms");
        System.out.println("总耗时(所有轮次): " + sum + " ms");
        System.out.println("吞吐量: " + String.format("%.2f", (double) executionTimes.size() / (totalTime / 1000.0)) + " 请求/秒");
    }
}