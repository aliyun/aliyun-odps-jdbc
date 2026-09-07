/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc.utils;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LoggerConcurrencyTest {
  @Test void oneSlowBackendCallDoesNotAcquireDriverWideMonitor() throws Exception {
    OdpsLogger logger = new OdpsLogger("offline-test", "test", null, null, false, false);
    org.slf4j.Logger backend = mock(org.slf4j.Logger.class);
    Field field = OdpsLogger.class.getDeclaredField("sl4jLogger");
    field.setAccessible(true);
    field.set(logger, backend);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(call -> { entered.countDown(); release.await(5, TimeUnit.SECONDS); return null; })
        .when(backend).info("slow");
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> slow = pool.submit(() -> logger.info("slow"));
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      pool.submit(() -> logger.info("fast")).get(2, TimeUnit.SECONDS);
      verify(backend).info("fast");
      release.countDown();
      slow.get(2, TimeUnit.SECONDS);
    } finally { release.countDown(); pool.shutdownNow(); }
  }
}
