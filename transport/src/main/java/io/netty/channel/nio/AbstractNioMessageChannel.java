/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.ServerChannel;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on messages.
 */
public abstract class AbstractNioMessageChannel extends AbstractNioChannel {
    boolean inputShutdown;

    /**
     * @see AbstractNioChannel#AbstractNioChannel(Channel, SelectableChannel, int)
     */
    protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, int readInterestOp) {
        super(parent, ch, readInterestOp);
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        // 关注 OP_ACCEPT 事件,对应 NioServerSocketChannel
        return new NioMessageUnsafe();
    }

    @Override
    protected void doBeginRead() throws Exception {
        if (inputShutdown) {
            return;
        }
        super.doBeginRead();
    }

    protected boolean continueReading(RecvByteBufAllocator.Handle allocHandle) {
        // DefaultMaxMessagesRecvByteBufAllocator.MaxMessageHandle.continueReading
        return allocHandle.continueReading();
    }

    private final class NioMessageUnsafe extends AbstractNioUnsafe {

        // 存放建立连接后，创建的 NioSocketChannel
        private final List<Object> readBuf = new ArrayList<Object>();

        /**
         * OP_ACCEPT
         */
        @Override
        public void read() {
            assert eventLoop().inEventLoop();

            // NioServerSocketChannel 中的 config, pipeline
            final ChannelConfig config = config();
            final ChannelPipeline pipeline = pipeline();

            // 创建接收数据的 Buffer分配器, 其为 byteBuf 分配大小合适的空间。
            // 在当前场景中(接收连接的场景中), 这里的 allocHandle 只是用于控制循环接受并创建连接的次数。
            final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
            allocHandle.reset(config);

            boolean closed = false;
            Throwable exception = null;
            try {
                try {
                    do {
                        // 对于 NioServerSocketChannel 来说, 就是接收一个客户端 Channel, 添加到 readBuf。
                        // 调用子类的实现的方法, 当成功读取时返回1。
                        // io.netty.channel.socket.nio.NioServerSocketChannel.doReadMessages
                        int localRead = doReadMessages(readBuf);
                        // 已无数据, 跳出循环
                        if (localRead == 0) {
                            break;
                        }
                        // 链路关闭, 跳出循环
                        if (localRead < 0) {
                            closed = true;
                            break;
                        }

                        // 递增已读取的消息数量
                        allocHandle.incMessagesRead(localRead);
                        // 循环不超过16次
                    } while (continueReading(allocHandle));
                } catch (Throwable t) {
                    exception = t;
                }

                // 循环处理 readBuf 中的 NioSocketChannel
                int size = readBuf.size();
                for (int i = 0; i < size; i++) {
                    readPending = false;

                    // 通过 NioServerSocketChannel 的 pipeline 传播 ChannelRead 事件
                    // {@link io.netty.bootstrap.ServerBootstrap.ServerBootstrapAcceptor#channelRead}
                    // 从 readBuf 中取出的是 NioSocketChannel
                    pipeline.fireChannelRead(readBuf.get(i));
                }
                readBuf.clear();
                // 读取完毕的回调, 有的Handle会根据本次读取的总字节数, 自适应调整下次应该分配的缓冲区大小
                allocHandle.readComplete();
                // 通过 pipeline 传播 ChannelReadComplete 事件
                pipeline.fireChannelReadComplete();

                if (exception != null) {
                    // 事件处理异常了

                    // 是否需要关闭连接
                    closed = closeOnReadError(exception);

                    // 通过pipeline传播异常事件
                    pipeline.fireExceptionCaught(exception);
                }

                if (closed) {
                    // 如果需要关闭, 那就关闭

                    inputShutdown = true;
                    if (isOpen()) {
                        close(voidPromise());
                    }
                }
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        final SelectionKey key = selectionKey();
        // 获取 Key 的兴趣集
        final int interestOps = key.interestOps();

        int maxMessagesPerWrite = maxMessagesPerWrite();
        // 当前校验最大写的次数是否大于0
        while (maxMessagesPerWrite > 0) {

            // 从 ChannelOutboundBuffer 中弹出一条消息进行处理
            Object msg = in.current();
            // 如果消息为空,说明发送缓冲区为空,所有消息都已经被发送完成,退出循环。
            if (msg == null) {
                break;
            }
            try {
                boolean done = false;
                // 获取配置中, 循环写的最大次数
                for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                    // 调用子方法进行循环写操作, 成功返回true
                    if (doWriteMessage(msg, in)) {
                        done = true;
                        break;
                    }
                }

                // 若发送成功, 则将其从缓存链表中移除, 继续发送循环获取下一个数据
                if (done) {
                    maxMessagesPerWrite--;
                    in.remove();
                } else {
                    break;
                }
            } catch (Exception e) {
                // 判断如果遇到异常是否要继续写
                if (continueOnWriteError()) {
                    maxMessagesPerWrite--;
                    in.remove(e);
                } else {
                    throw e;
                }
            }
        }
        if (in.isEmpty()) {
            // 数据已全部发送发送完成, 从兴趣集中移除 OP_WRITE 事件
            if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
            }
        } else {
            // 如果数据还没写完, 将OP_WRITE加入到兴趣集中
            if ((interestOps & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(interestOps | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Returns {@code true} if we should continue the write loop on a write error.
     */
    protected boolean continueOnWriteError() {
        return false;
    }

    protected boolean closeOnReadError(Throwable cause) {
        if (!isActive()) {
            // If the channel is not active anymore for whatever reason we should not try to continue reading.
            return true;
        }
        if (cause instanceof PortUnreachableException) {
            return false;
        }
        if (cause instanceof IOException) {
            // ServerChannel should not be closed even on IOException because it can often continue
            // accepting incoming connections. (e.g. too many open files)
            return !(this instanceof ServerChannel);
        }
        return true;
    }

    /**
     * Read messages into the given array and return the amount which was read.
     */
    protected abstract int doReadMessages(List<Object> buf) throws Exception;

    /**
     * Write a message to the underlying {@link java.nio.channels.Channel}.
     *
     * @return {@code true} if and only if the message has been written
     */
    protected abstract boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception;
}
