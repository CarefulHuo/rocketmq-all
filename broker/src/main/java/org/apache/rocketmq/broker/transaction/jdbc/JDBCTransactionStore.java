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

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.transaction.TransactionRecord;
import org.apache.rocketmq.broker.transaction.TransactionStore;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * JDBCTransactionStore是一个实现了TransactionStore接口的类，用于存储事务消息的元数据。它通过JDBC连接数据库，并且支持事务的增删查改操作。
 * JDBCTransactionStore类的成员变量包括一个JDBCTransactionStoreConfig对象（用于存储数据库连接配置信息），一个Connection对象（用于建立数据库连接），以及一个AtomicLong对象（用于记录总记录数）。
 * JDBCTransactionStore类的构造函数接收一个JDBCTransactionStoreConfig对象作为参数，用于初始化数据库连接配置信息。
 * open()方法用于打开数据库连接，并且自动创建数据库表。它首先尝试加载数据库驱动，然后根据配置信息创建数据库连接，并设置自动提交为false。接着，它会尝试计算总记录数，如果计算失败则尝试创建数据库表。最后，如果连接创建成功，则返回true，否则返回false。
 * loadDriver()方法用于加载数据库驱动。它尝试根据配置信息中的驱动类名加载驱动，并返回加载结果。
 * computeTotalRecords()方法用于计算总记录数。它创建一个Statement对象，并执行SQL查询语句，将结果保存在ResultSet对象中。接着，它从结果集中获取总记录数并将其保存在totalRecordsValue中，最后关闭Statement和ResultSet对象并返回计算结果。
 * createDB()方法用于创建数据库表。它创建一个Statement对象，并执行SQL创建表语句，最后提交事务并返回创建结果。
 * createTableSql()方法用于读取SQL创建表语句。它从类路径下的transaction.sql文件中读取语句并返回。
 * close()方法用于关闭数据库连接。
 * put()方法用于插入事务消息的元数据。它创建一个PreparedStatement对象，并根据传入的TransactionRecord对象列表执行批量插入操作，最后提交事务并返回插入结果。
 * remove()方法用于删除事务消息的元数据。它创建一个PreparedStatement对象，并根据传入的主键列表执行批量删除操作，最后提交事务。
 * traverse()方法用于遍历事务消息的元数据。当前实现返回空列表。
 * totalRecords()方法用于获取事务消息的总记录数。它返回totalRecordsValue的值。
 * minPK()和maxPK()方法用于获取最小和最大主键值。当前实现均返回0
 */
public class JDBCTransactionStore implements TransactionStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
    private Connection connection;
    private AtomicLong totalRecordsValue = new AtomicLong(0);

    public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig) {
        this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
    }

    @Override
    public boolean open() {
        if (this.loadDriver()) {
            Properties props = new Properties();
            props.put("user", jdbcTransactionStoreConfig.getJdbcUser());
            props.put("password", jdbcTransactionStoreConfig.getJdbcPassword());

            try {
                this.connection =
                    DriverManager.getConnection(this.jdbcTransactionStoreConfig.getJdbcURL(), props);

                this.connection.setAutoCommit(false);

                if (!this.computeTotalRecords()) {
                    return this.createDB();
                }

                return true;
            } catch (SQLException e) {
                log.info("Create JDBC Connection Exception", e);
            }
        }

        return false;
    }

    private boolean loadDriver() {
        try {
            Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
            log.info("Loaded the appropriate driver, {}",
                this.jdbcTransactionStoreConfig.getJdbcDriverClass());
            return true;
        } catch (Exception e) {
            log.info("Loaded the appropriate driver Exception", e);
        }

        return false;
    }

    private boolean computeTotalRecords() {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = this.connection.createStatement();

            resultSet = statement.executeQuery("select count(offset) as total from t_transaction");
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            this.totalRecordsValue.set(resultSet.getLong(1));
        } catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }

            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }

        return true;
    }

    private boolean createDB() {
        Statement statement = null;
        try {
            statement = this.connection.createStatement();

            String sql = this.createTableSql();
            log.info("createDB SQL:\n {}", sql);
            statement.execute(sql);
            this.connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    log.warn("Close statement exception", e);
                }
            }
        }
    }

    private String createTableSql() {
        URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
        String fileContent = MixAll.file2String(resource);
        return fileContent;
    }

    @Override
    public void close() {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
        }
    }

    @Override
    public boolean put(List<TransactionRecord> trs) {
        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("insert into t_transaction values (?, ?)");
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    log.warn("Close statement exception", e);
                }
            }
        }
    }

    private long updatedRows(int[] rows) {
        long res = 0;
        for (int i : rows) {
            res += i;
        }

        return res;
    }

    @Override
    public void remove(List<Long> pks) {
        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("DELETE FROM t_transaction WHERE offset = ?");
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        return null;
    }

    @Override
    public long totalRecords() {
        return this.totalRecordsValue.get();
    }

    @Override
    public long minPK() {
        return 0;
    }

    @Override
    public long maxPK() {
        return 0;
    }
}
