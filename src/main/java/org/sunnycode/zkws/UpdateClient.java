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
package org.sunnycode.zkws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunnycode.zkws.socketio.impl.IOAcknowledge;
import org.sunnycode.zkws.socketio.impl.IOCallback;
import org.sunnycode.zkws.socketio.impl.SocketIO;
import org.sunnycode.zkws.socketio.impl.SocketIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

public class UpdateClient implements IOCallback {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String serviceUrl;
  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<String, List<UpdateListener>> listeners;
  private final SocketIO socket;

  public UpdateClient(String serviceUrl) {
    this.serviceUrl = serviceUrl;
    this.listeners = new LinkedHashMap<String, List<UpdateListener>>();

    try {
      this.socket = new SocketIO(this.serviceUrl, this);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public synchronized void watch(String path, UpdateListener listener) {
    if (!this.listeners.containsKey(path)) {
      this.listeners.put(path, new ArrayList<UpdateListener>());
    }

    this.listeners.get(path).add(listener);
    this.socket.emit("watch", ImmutableMap.of("path", path));
  }

  @Override
  public void onConnect() {
    log.debug("connected");
  }

  @Override
  public void onDisconnect() {
    log.debug("disconnected");
  }

  @Override
  public void onError(SocketIOException socketIOException) {
    socketIOException.printStackTrace();
    log.debug("error");
  }

  @Override
  public void onMessage(Map<String, Object> json, IOAcknowledge ack) {
    ack.ack();
    log.debug("message: {}", json);
  }

  @Override
  public void onMessage(String data, IOAcknowledge ack) {
    ack.ack();
    log.debug("message: {}", data);
  }

  @Override
  public void on(String event, IOAcknowledge ack, Object... args) {
    log.debug("{}: {}", event, Arrays.asList(args));

    if ("update".equals(event)) {
      if (ack != null) {
        ack.ack();
      }

      try {
        Map<String, Object> details = (Map<String, Object>) args[0];

        if (details == null || !details.containsKey("path")) {
          return;
        }

        String path = (String) details.get("path");

        Map<String, Object> value =
            details.containsKey("value") ? mapper.readValue((String) details.get("value"),
                LinkedHashMap.class) : null;

        for (UpdateListener listener : listeners.get(path)) {
          listener.updated(path, value);
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

//  public static void main(String[] args) {
//    UpdateClient client = new UpdateClient("http://localhost:8080/");
//    client.watch("/foo", new UpdateListener() {
//      @Override
//      public void updated(String path, Map<String, Object> value) {
//        System.out.println("updated! " + path + " -> " + value);
//      }
//    });
//    client.watch("/bar", new UpdateListener() {
//      @Override
//      public void updated(String path, Map<String, Object> value) {
//        System.out.println("updated! " + path + " -> " + value);
//      }
//    });
//  }
}
