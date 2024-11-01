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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    /**
     * MappedFile 的引用次数
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;

    /**
     * MappedFile 第一次关闭时设置的时间戳
     */
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     */
    public void shutdown(final long intervalForcibly) {
        // 如果可用，则设置为非可用
        if (this.available) {
            this.available = false;
            // 设置关闭时间戳
            this.firstShutdownTimestamp = System.currentTimeMillis();

            // 释放资源
            // 只有在 MappedFile 的引用次数小于 1 的情况下，才会释放资源
            this.release();

            // 如果当前该文件被其他线程引用，则本次不强制删除
        } else if (this.getRefCount() > 0) {

            // 在拒绝被删除保护期外，每执行一次清理任务，将引用次数减去 1000 引用次数小于 1 之后，该文件最终将删除
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
