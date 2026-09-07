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

import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LoggerBridgeContractTest {
  @Test void driverOwnedJulHandlerDoesNotAlsoForwardToRootBridge() throws Exception {
    String name = "offline.bridge." + UUID.randomUUID();
    Path output = Files.createTempFile("jdbc-log-contract", ".log");
    AtomicInteger rootCalls = new AtomicInteger();
    Handler rootBridge = new Handler() {
      public void publish(LogRecord record) { if (name.equals(record.getLoggerName())) rootCalls.incrementAndGet(); }
      public void flush() { }
      public void close() { }
    };
    Logger root = Logger.getLogger("");
    root.addHandler(rootBridge);
    try {
      OdpsLogger logger = new OdpsLogger(name, "test", output.toString(), null, false, true);
      logger.info("one event");
      assertEquals(0, rootCalls.get());
      assertTrue(new String(Files.readAllBytes(output), java.nio.charset.StandardCharsets.UTF_8).contains("one event"));
    } finally {
      root.removeHandler(rootBridge);
      for (Handler handler : Logger.getLogger(name).getHandlers()) {
        handler.close(); Logger.getLogger(name).removeHandler(handler);
      }
      Files.deleteIfExists(output);
    }
  }
}
