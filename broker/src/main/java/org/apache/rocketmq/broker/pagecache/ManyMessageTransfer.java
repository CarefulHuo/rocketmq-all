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
package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * 在Broker服务端进行消息传输时，用于封装消息体和消息头信息的一个类，实现了FileRegion接口。该类的主要作用是将消息头和消息体作为一个整体进行传输，支持分批次传输消息体，并提供了消息传输的位置、已传输的字节数、总字节数等信息。
 * 具体实现细节如下：
 * ManyMessageTransfer 继承自 AbstractReferenceCounted，并实现了 FileRegion 接口。AbstractReferenceCounted 是 Netty 中的一个类，用于实现资源的引用计数，确保资源被正确地释放。
 * ManyMessageTransfer 类包含两个成员变量：byteBufferHeader 用于存储消息头信息的 ByteBuffer 对象，getMessageResult 用于存储消息体信息的 GetMessageResult 对象。
 * transferred 属性用于记录已经传输的字节数。
 * position() 方法用于获取当前传输的位置，即已经传输的字节数。
 * transfered() 方法用于获取已经传输的字节数。
 * count() 方法用于获取需要传输的总字节数，即消息头和消息体的总长度。
 * transferTo() 方法用于将消息传输到指定的 WritableByteChannel，实现了消息的分批次传输。该方法首先尝试传输消息头，如果消息头已经传输完毕，则尝试传输消息体。如果消息体也已经传输完毕，则返回0，表示没有更多的数据需要传输。
 * close() 方法用于释放资源，调用 deallocate() 方法释放 GetMessageResult 对象所占用的资源。
 * deallocate() 方法用于释放 GetMessageResult 对象所占用的资源，调用 getMessageResult.release() 方法释放资源。
 */
public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final GetMessageResult getMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public ManyMessageTransfer(ByteBuffer byteBufferHeader, GetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return byteBufferHeader.limit() + this.getMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    transferred += target.write(bb);
                    return transferred;
                }
            }
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.getMessageResult.release();
    }
}
