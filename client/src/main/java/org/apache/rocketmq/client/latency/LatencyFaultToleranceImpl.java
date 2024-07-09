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

package org.apache.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;

/**
 * 延迟故障容错实现类，维护每个对象的信息
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

    /**
     * 规避对象集合
     * key：不可用 Broker 的名称
     * value：失败条目
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);

    /**
     * 对象选择 index
     *
     * @see LatencyFaultToleranceImpl#pickOneAtLeast()
     */
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    /**
     *
     * @param name                 Broker 名称
     * @param currentLatency       消息发送故障的延迟时间
     * @param notAvailableDuration 不可用持续时长，在这个时间内，Broker 将被规避
     */
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            // 创建对象
            final FaultItem faultItem = new FaultItem(name);

            // 延迟时间
            faultItem.setCurrentLatency(currentLatency);

            // 开始可用时间
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);

            // 更新对象
            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }

            // 更新对象
        } else {
            old.setCurrentLatency(currentLatency);
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    /**
     * 是否可用，即是否到达可用时间
     *
     * @param name Broker名称
     * @return
     */
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    /**
     * 移除失败条目，意味着 Broker 重新参与路由计算
     *
     * @param name Broker 名称
     */
    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 尝试从规避的 Broker 中选择一个可用的 Broker，如果没有找到，返回 null
     *
     * @return
     */
    @Override
    public String pickOneAtLeast() {
        // 将 faultItemTable 集合中的元素 创建数组
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            Collections.shuffle(tmpList);

            Collections.sort(tmpList);

            // 选择顺序在前一半的对象，这样是为了找到失败条目最少得 Broker
            final int half = tmpList.size() / 2;
            if (half <= 0) {
                return tmpList.get(0).getName();
            } else {
                final int i = this.whichItemWorst.incrementAndGet() % half;
                return tmpList.get(i).getName();
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    /**
     * 失败条目，维护失败条目对象的名字、延迟、开始可用的时间
     */
    class FaultItem implements Comparable<FaultItem> {
        /**
         * 条目名，这里是 Broker 名称
         */
        private final String name;

        /**
         * 消息发送故障的延迟时间
         */
        private volatile long currentLatency;

        /**
         * Broker 可用的开始时间
         */
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * Broker 是否可用，当开始可用时间大于当前时间
         *
         * @return
         */
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
