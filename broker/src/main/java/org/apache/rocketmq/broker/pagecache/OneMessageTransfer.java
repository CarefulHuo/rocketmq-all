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
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 该类继承自AbstractReferenceCounted并实现了FileRegion接口。它的主要作用是用于传输消息体，将消息从内存中的ByteBuffer和SelectMappedBufferResult传输到目标WritableByteChannel。
 * OneMessageTransfer类有一个ByteBuffer类型的字段byteBufferHeader和一个SelectMappedBufferResult类型的字段selectMappedBufferResult，用于存储消息头和消息体。
 * transferred字段用于记录已经传输的字节数。
 * 构造函数OneMessageTransfer用于初始化byteBufferHeader和selectMappedBufferResult。
 * position()方法返回当前传输的位置，即byteBufferHeader和selectMappedBufferResult中已经传输的字节数之和。
 * transfered()方法返回已经传输的字节数。
 * count()方法返回待传输的总字节数，即byteBufferHeader的剩余字节数加上selectMappedBufferResult的大小。
 * transferTo()方法用于将消息体传输到目标WritableByteChannel，根据byteBufferHeader和selectMappedBufferResult的剩余字节数分别进行传输，直到所有字节都传输完成或没有剩余字节可传输。
 * close()方法用于释放资源，调用deallocate()方法。
 * deallocate()方法用于释放selectMappedBufferResult所占用的资源，通过调用selectMappedBufferResult.release()方法。
 */
public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final SelectMappedBufferResult selectMappedBufferResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMappedBufferResult selectMappedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMappedBufferResult = selectMappedBufferResult;
    }

    @Override
    public long position() {
        return this.byteBufferHeader.position() + this.selectMappedBufferResult.getByteBuffer().position();
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMappedBufferResult.getSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else if (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            transferred += target.write(this.selectMappedBufferResult.getByteBuffer());
            return transferred;
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.selectMappedBufferResult.release();
    }
}
