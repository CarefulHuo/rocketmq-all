/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.transaction.jdbc;

/**
 * 用于配置JDBC事务存储。
 * 包含了JDBC驱动类名、数据库URL、数据库用户名和密码等属性，
 * 并提供了相应的getter和setter方法用于获取和设置这些属性的值。这个配置类主要用于在Broker中使用JDBC事务日志存储时的配置。
 */
public class JDBCTransactionStoreConfig {
    private String jdbcDriverClass = "com.mysql.jdbc.Driver";
    private String jdbcURL = "jdbc:mysql://xxx.xxx.xxx.xxx:1000/xxx?useUnicode=true&characterEncoding=UTF-8";
    private String jdbcUser = "xxx";
    private String jdbcPassword = "xxx";

    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }

    public void setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }
}
