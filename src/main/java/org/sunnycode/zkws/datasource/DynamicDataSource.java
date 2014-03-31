/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.sunnycode.zkws.datasource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunnycode.zkws.UpdateClient;
import org.sunnycode.zkws.UpdateListener;
import org.sunnycode.zkws.util.MaskProxy;

import com.google.common.base.Throwables;

public abstract class DynamicDataSource implements UpdateListener, Closeable {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Random random = new Random();

  private final MaskProxy<DataSource, DataSource> instance;
  private final UpdateClient updateClient;
  private final String zkPath;

  public DynamicDataSource(String serviceUrl, String zkPath) {
    this.updateClient = new UpdateClient(serviceUrl);
    this.instance = new MaskProxy<DataSource, DataSource>(DataSource.class, null);
    this.zkPath = zkPath;

    updateClient.watch(zkPath, this);
  }

  @Override
  public synchronized void updated(String path, Map<String, Object> properties) {
    if (!zkPath.equals(path)) {
      return;
    }

    log.info("configuration updating [{}]", path);

    try {
      DataSource newInstance = createDataSource(properties);

      doSleep(properties);

      DataSource original = this.instance.getAndSet(newInstance);

      doClose(original);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    log.debug("configuration updated [{}]", path);
  }

  public DataSource get() {
    return instance.asProxyInstance();
  }

  @Override
  public void close() throws IOException {
    log.debug("closing datasource");

    DataSource original = this.instance.getAndSet(null);
    doClose(original);
  }

  protected abstract DataSource createDataSource(Map<String, Object> properties);

  private void doSleep(Map<String, Object> properties) {
    Integer jitterMinMs = 0;
    Integer jitterMaxMs = 0;

    if (properties.containsKey("jitterMinMs")) {
      jitterMinMs = ((Number) properties.get("jitterMinMs")).intValue();
    }

    if (properties.containsKey("jitterMaxMs")) {
      jitterMaxMs = ((Number) properties.get("jitterMaxMs")).intValue();
    }

    if (jitterMaxMs <= 0) {
      return;
    }

    Integer delay = jitterMinMs + random.nextInt(jitterMaxMs - jitterMinMs);

    try {
      log.debug("sleeping {} ms", delay);

      Thread.sleep(delay);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void doClose(DataSource instance) {
    if (instance != null && instance instanceof Closeable) {
      try {
        ((Closeable) instance).close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
