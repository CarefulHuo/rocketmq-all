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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageQueue;

public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    /**
     * 锁的最大存活时间，默认 60 s
     */
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    /**
     * 消费者组下 MessageQueue 的锁定映射表，按消费组分组
     * todo
     * 1. 不同消费组的消费者，可以同时锁定同一个消费队列
     * 2. 集群模式下，同一个消费队列，在同一个消费组内只能被一个消费者锁定
     * todo 本质上是同一消费组内的消费者锁定 MessageQueue
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable = new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    /**
     * 尝试锁住 MessageQueue
     *
     * @param group    消费组
     * @param mq       消息队列
     * @param clientId 消费组下某个消费端Id
     * @return
     */
    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        // 判断是否已经锁住，锁住的话，刷新过期时间为当前时间，本质是刷新过期时间，将锁进行延期
        // 没有锁住的话，1) 当前 Group-MQ 下的锁对象不是传入的 ClientId 2) 是传入的 ClientId 但是锁过期了
        if (!this.isLocked(group, mq, clientId)) {
            try {
                // 获取 JVM 实例锁
                // todo 因为每个 Broker 维护自己的队列锁，并不共享
                this.lock.lockInterruptibly();
                try {
                    // 尝试获取，判断消费组否存在
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        // 不存在，创建组对象
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 根据消费队列获取锁对象
                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        // 不存在，则创建锁对象，放入 ClientId，放入锁过期时间
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}", group, clientId, mq);
                    }

                    // 占据锁对象的是传入的 ClientId，锁住状态下，刷新过期时间为当前时间，即锁延期，本质上就是延长锁的过期时间
                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    // 锁对象不是传入的 ClientId，那么需要判断这个锁对象是否过期
                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        // 如果过期了，就可以重置锁对象，即传入的 ClientId，就可以占据了
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn("tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                        return true;
                    }

                    log.warn("tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                    // group 下别的 ClientId 占据了队列的锁对象，而且锁没有过期，那么就无法获取锁
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 判断消费组 Group 下的消息队列 MQ 是否被 ClientId 锁定，可能情况如下：
     * <p>
     * 1. 当前 Group 下还没有客户端实例锁定这个 MQ，那么传入的 ClientId 直接锁定即可；
     * 2. 当前 Group 下正好是传入的 ClientId 锁定的，而且锁还没过期，那么直接刷新过期时间即可；
     * 3. 当前 Group 下正好是传入的 ClientId 锁定的，但是锁过期了，那么重置过期时间即可；
     * 4. 当前 Group 下锁住 MQ 的不是传入的 ClientId，而是其他客户端：
     * - 如果其他客户端持有该 mq 的LockEntry，且没有过期，那么传入的 ClientId 就会获取锁失败
     * - 如果其他客户端持有该 mq 的LockEntry，且已经过期，那么传入的 ClientId 就会获取锁成功，即传入的 ClientId 可以占据 MQ 的 LockEntry
     * </p>
     *
     * @param group    消费组
     * @param mq       消息队列
     * @param clientId 消费组下某个消费端Id
     * @return
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        // 取出当前 Broker 上指定消费组 Group 下的消息队列锁定表
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            // 获取 消息队列的锁对象
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                // 判断 是否被传入的 ClientId 锁定 && 锁是否过期
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    // 锁有效，刷新过期时间
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * ClientId 尝试批量获取消息队列的锁
     *
     * @param group    消费组
     * @param mqs      消息队列集合
     * @param clientId 消费组下某个消费端Id
     * @return
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        // 锁定成功的 MessageQueue 集合结果集
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        // 锁定失败的 MessageQueue 集合结果集
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 遍历需要锁定的消息队列
        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                // 判断传入的clientId，是否已经锁定了当前队列，如果锁定了，则加入到锁定成功的集合中
                lockedMqs.add(mq);
            } else {
                // 如果没有锁定，则加入到锁定失败的集合中
                notLockedMqs.add(mq);
            }
        }

        // 存在没有被锁定的消息队列，那么尝试获取锁
        if (!notLockedMqs.isEmpty()) {
            try {
                // 获取 JVM 实例锁
                this.lock.lockInterruptibly();
                try {
                    // 消费组不在锁定的 map , 加入锁定的 map 中
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 对没有锁定的 MessageQueue ，尝试用当前的 ClientId 锁定消息队列
                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            // 创建锁对象
                            lockEntry = new LockEntry();
                            // 设置锁对象，维护哪个 ClientId 锁定了该消息队列
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info("tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}", group, clientId, mq);
                        }

                        // 如果当前 ClientId 锁定了该消息队列，刷新过期时间，并加入到锁定成功的集合中
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        // 如果当前 ClientId 没有锁定该消息队列，但是锁已经过期，那么尝试用当前 ClientId 锁定该消息队列
                        String oldClientId = lockEntry.getClientId();
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn("tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn("tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }
        // 返回锁定的 MessageQueue 集合
        return lockedMqs;
    }

    /**
     * ClientId 尝试批量释放自己持有的 MessageQueue 的锁，不是自己持有的，不能释放
     *
     * @param group    消费组
     * @param mqs      消息队列集合
     * @param clientId 消费组下某个消费端Id
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            // JVM 实例锁
            this.lock.lockInterruptibly();
            try {
                // 获取当前 Broker 上的消费组的消息队列锁定情况
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    // 遍历需要释放的 MessageQueue
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                                // 存在该 mq 的锁，且该锁的 clientId 是当前 clientId，那么释放该锁
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}", group, mq, clientId);
                            } else {
                                // 不是该 clientId 持有的锁，不能释放
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}", lockEntry.getClientId(), group, mq, clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}", group, mq, clientId);
                        }
                    }
                } else {
                    // 不存在该消费组的消息队列锁，不需要释放
                    log.warn("unlockBatch, group not exist, Group: {} {}", group, clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    /**
     * MessageQueue 对应锁对象
     */
    static class LockEntry {
        /**
         * 消费端实例Id
         */
        private String clientId;

        /**
         * 锁对象更新时间，使用 volatile 修饰，是为了保证锁过期时间的刷新或重置的原子性
         */
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        /**
         * 重置锁过期时间
         * @param lastUpdateTimestamp
         */
        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 是否锁定
         * @param clientId
         * @return
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        /**
         * 锁是否过期，根据当前时间和锁上次更新时间差，判断是否超过阈值
         * @return
         */
        public boolean isExpired() {
            boolean expired = (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
