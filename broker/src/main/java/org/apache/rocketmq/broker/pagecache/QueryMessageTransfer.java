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
import org.apache.rocketmq.store.QueryMessageResult;

/**
 * 在Broker服务端将查询消息的结果传输给客户端。它实现了FileRegion接口，可以将数据通过transferTo方法传输到指定的WritableByteChannel中。
 * QueryMessageTransfer类继承自AbstractReferenceCounted，表示它是一个可引用计数的对象，可以控制其生命周期。
 * 构造函数QueryMessageTransfer接受一个ByteBuffer对象和一个QueryMessageResult对象作为参数，用于构建消息传输对象。
 * position()方法返回当前传输的位置，即已经传输的字节数。
 * transfered()方法返回已经传输的字节数。
 * count()方法返回待传输的总字节数。
 * transferTo()方法将数据传输到指定的WritableByteChannel中，直到没有剩余数据可传输。
 * close()方法释放资源，调用deallocate()方法。
 * deallocate()方法释放QueryMessageResult对象所占用的资源。
 */
public class QueryMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final QueryMessageResult queryMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public QueryMessageTransfer(ByteBuffer byteBufferHeader, QueryMessageResult queryMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.queryMessageResult = queryMessageResult;
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
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
        return byteBufferHeader.limit() + this.queryMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
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
        this.queryMessageResult.release();
    }
}
