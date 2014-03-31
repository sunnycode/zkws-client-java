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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import javax.sql.DataSource;

import com.google.common.base.Throwables;
import com.jolbox.bonecp.BoneCPDataSource;

public class DynamicDataSourceBoneCpImpl extends DynamicDataSource {
  public DynamicDataSourceBoneCpImpl(String serviceUrl, String zkPath) {
    super(serviceUrl, zkPath);
  }

  protected DataSource createDataSource(Map<String, Object> properties) {
    BoneCPDataSource dataSource = new BoneCPDataSource();

    try {
      Class.forName((String) properties.get("driver"));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    dataSource.setDriverClass((String) properties.get("driver"));
    dataSource.setJdbcUrl((String) properties.get("jdbcUrl"));
    dataSource.setUsername((String) properties.get("username"));
    dataSource.setPassword((String) properties.get("password"));

    dataSource
        .setMinConnectionsPerPartition(((Number) properties.get("minConnections")).intValue());
    dataSource
        .setMaxConnectionsPerPartition(((Number) properties.get("maxConnections")).intValue());

    dataSource
        .setMinConnectionsPerPartition(((Number) properties.get("minConnections")).intValue());
    dataSource
        .setMaxConnectionsPerPartition(((Number) properties.get("maxConnections")).intValue());

    return dataSource;
  }

//  public static void main(String[] args) throws Exception {
//    DynamicDataSourceBoneCpImpl dsProvider =
//        new DynamicDataSourceBoneCpImpl("http://localhost:8080", "/foo");
//    DataSource ds = dsProvider.get();
//
//    while (true) {
//      try (Connection c = ds.getConnection()) {
//        PreparedStatement stmt = c.prepareStatement("select @@server_id");
//        ResultSet rs = stmt.executeQuery();
//        if (rs.next()) {
//          System.out.println("got: " + rs.getString(1));
//        } else {
//          break;
//        }
//      } catch (Exception ok) {
//        ok.printStackTrace();
//      }
//
//      Thread.sleep(300);
//    }
//
//    try {
//      dsProvider.close();
//    } catch (Exception ignored) {}
//  }
}
