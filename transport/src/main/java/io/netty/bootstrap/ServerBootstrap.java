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
package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link Bootstrap} sub-class which allows easy bootstrap of {@link ServerChannel}
 *
 */
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ServerBootstrap.class);

    // The order in which child ChannelOptions are applied is important they may depend on each other for validation
    // purposes.
    private final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<ChannelOption<?>, Object>();
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<AttributeKey<?>, Object>();
    private final ServerBootstrapConfig config = new ServerBootstrapConfig(this);

    /**
     * childGroup 用于处理每一个已建立连接发生的I/O读写事件
     */
    private volatile EventLoopGroup childGroup;
    private volatile ChannelHandler childHandler;

    public ServerBootstrap() { }

    private ServerBootstrap(ServerBootstrap bootstrap) {
        super(bootstrap);
        childGroup = bootstrap.childGroup;
        childHandler = bootstrap.childHandler;
        synchronized (bootstrap.childOptions) {
            childOptions.putAll(bootstrap.childOptions);
        }
        childAttrs.putAll(bootstrap.childAttrs);
    }

    /**
     * Specify the {@link EventLoopGroup} which is used for the parent (acceptor) and the child (client).
     */
    @Override
    public ServerBootstrap group(EventLoopGroup group) {
        /*
            如果只使用一个线程池，即 parentGroup 和 childGroup 是同一个线程池,
            '主从reactor-多线程模型' 退化为-> '单reactor-单线程模型' or '单reactor-多线程模型'

            具体是 '单reactor-单线程模型' 还是 '单reactor-多线程模型'，要看构造EventLoopGroup时的配置。
         */
        return group(group, group);
    }

    /**
     * Set the {@link EventLoopGroup} for the parent (acceptor) and the child (client). These
     * {@link EventLoopGroup}'s are used to handle all the events and IO for {@link ServerChannel} and
     * {@link Channel}'s.
     *
     * @param parentGroup 用于监听客户端连接,专门负责与客户端创建连接
     * @param childGroup  用于处理每一个已建立连接发生的I/O读写事件
     */
    public ServerBootstrap group(EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        super.group(parentGroup);
        if (this.childGroup != null) {
            throw new IllegalStateException("childGroup set already");
        }
        this.childGroup = ObjectUtil.checkNotNull(childGroup, "childGroup");
        return this;
    }

    /**
     * Allow to specify a {@link ChannelOption} which is used for the {@link Channel} instances once they get created
     * (after the acceptor accepted the {@link Channel}). Use a value of {@code null} to remove a previous set
     * {@link ChannelOption}.
     */
    public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) {
        ObjectUtil.checkNotNull(childOption, "childOption");
        synchronized (childOptions) {
            if (value == null) {
                childOptions.remove(childOption);
            } else {
                childOptions.put(childOption, value);
            }
        }
        return this;
    }

    /**
     * Set the specific {@link AttributeKey} with the given value on every child {@link Channel}. If the value is
     * {@code null} the {@link AttributeKey} is removed
     */
    public <T> ServerBootstrap childAttr(AttributeKey<T> childKey, T value) {
        ObjectUtil.checkNotNull(childKey, "childKey");
        if (value == null) {
            childAttrs.remove(childKey);
        } else {
            childAttrs.put(childKey, value);
        }
        return this;
    }

    /**
     * Set the {@link ChannelHandler} which is used to serve the request for the {@link Channel}'s.
     */
    public ServerBootstrap childHandler(ChannelHandler childHandler) {
        this.childHandler = ObjectUtil.checkNotNull(childHandler, "childHandler");
        return this;
    }

    /**
     * 初始化 channel
     * @param channel NioServerSocketChannel,由 serverBootstrap.channel 方法设置
     */
    @Override
    void init(Channel channel) {
        /*
            为 NioServerSocketChannel 配置TCP等参数

            newOptionsArray()方法返回的就是 由 serverBootstrap.option 方法添加的参数
            @see io.netty.bootstrap.AbstractBootstrap.option
         */
        setChannelOptions(channel, newOptionsArray(), logger);

        /*
            为 NioServerSocketChannel 配置自定义属性

            newAttributesArray()方法返回的就是 由 serverBootstrap.attr 方法添加的 自定义属性
            @see io.netty.bootstrap.AbstractBootstrap.attr
         */
        setAttributes(channel, newAttributesArray());

        // 从 NioServerSocketChannel 中取出 pipeline
        ChannelPipeline p = channel.pipeline();

        // 以下四个参数用于初始化 childGroup 中的 child,即:用于处理每一个已建立连接发生的I/O读写事件

        // 获取 childGroup,即: 用于处理每一个已建立连接发生的I/O读写事件
        final EventLoopGroup currentChildGroup = childGroup;
        final ChannelHandler currentChildHandler = childHandler;
        final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
        final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);

        /*
            装配 pipeline 流水线

            ChannelInitializer 一次性、初始化handler
                它会添加 ServerBootstrapAcceptor handler，添加完成后自己就移除了。
                ServerBootstrapAcceptor handler 负责与客户端建立连接
         */
        p.addLast(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(final Channel ch) {
                // 注意：这里的ch和上面的channel是同一个对象,即:NioServerSocketChannel

                // 从 NioServerSocketChannel 中取出 pipeline
                final ChannelPipeline pipeline = ch.pipeline();

                // 将由 serverBootstrap.handler 配置的handler,添加到 NioServerSocketChannel 的 pipeline 中。
                ChannelHandler handler = config.handler();
                if (handler != null) {
                    pipeline.addLast(handler);
                }

                // 向 NioServerSocketChannel(AbstractNioChannel) 所属的 EventLoop 提交一个异步任务
                ch.eventLoop().execute(new Runnable() {
                    @Override
                    public void run() {
                        // ServerBootstrapAcceptor 用于将建立连接的 SocketChannel 转发给 childGroup
                        pipeline.addLast(new ServerBootstrapAcceptor(
                                ch, currentChildGroup, currentChildHandler, currentChildOptions, currentChildAttrs));
                    }
                });
            }
        });
    }

    @Override
    public ServerBootstrap validate() {
        super.validate();
        if (childHandler == null) {
            throw new IllegalStateException("childHandler not set");
        }
        if (childGroup == null) {
            logger.warn("childGroup is not set. Using parentGroup instead.");
            childGroup = config.group();
        }
        return this;
    }

    private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

        /**
         * 用于处理每一个已建立连接发生的I/O读写事件
         */
        private final EventLoopGroup childGroup;

        /**
         * 由 serverBootstrap.childHandler 方法设置
         */
        private final ChannelHandler childHandler;

        /**
         * 由 serverBootstrap.childOption 方法设置
         */
        private final Entry<ChannelOption<?>, Object>[] childOptions;

        /**
         * 由 serverBootstrap.childAttr 方法设置
         */
        private final Entry<AttributeKey<?>, Object>[] childAttrs;
        private final Runnable enableAutoReadTask;

        /**
         * @param channel      NioServerSocketChannel,由 serverBootstrap.channel 方法设置
         * @param childGroup   用于处理每一个已建立连接发生的I/O读写事件
         * @param childHandler 由 serverBootstrap.childHandler 方法设置
         * @param childOptions 由 serverBootstrap.childOption 方法设置
         * @param childAttrs   由 serverBootstrap.childAttr 方法设置
         */
        ServerBootstrapAcceptor(
                final Channel channel, EventLoopGroup childGroup, ChannelHandler childHandler,
                Entry<ChannelOption<?>, Object>[] childOptions, Entry<AttributeKey<?>, Object>[] childAttrs) {
            this.childGroup = childGroup;
            this.childHandler = childHandler;
            this.childOptions = childOptions;
            this.childAttrs = childAttrs;

            // Task which is scheduled to re-enable auto-read.
            // It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
            // not be able to load the class because of the file limit it already reached.
            //
            // See https://github.com/netty/netty/issues/1328
            enableAutoReadTask = new Runnable() {
                @Override
                public void run() {
                    channel.config().setAutoRead(true);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // 此处的 msg,是已建立链接的 socketChannel,因此可以直接强转
            final Channel child = (Channel) msg;

            // 将 childHandler 添加到 已建立连接 channel 的 pipeline 中。
            child.pipeline().addLast(childHandler);

            setChannelOptions(child, childOptions, logger);
            setAttributes(child, childAttrs);

            try {
                /*
                    将已建立链接的 socketChannel 注册到 childGroup

                    childGroup 是如何从内部取出一个 child，与已建立链接的 socketChannel 绑定的?
                        其内部使用选择器(chooser),用于选择一个内部的 EventLoop.
                        源码位于: io.netty.util.concurrent.MultithreadEventExecutorGroup.next
                 */
                childGroup.register(child).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            forceClose(child, future.cause());
                        }
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }

        private static void forceClose(Channel child, Throwable t) {
            child.unsafe().closeForcibly();
            logger.warn("Failed to register an accepted channel: {}", child, t);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final ChannelConfig config = ctx.channel().config();
            if (config.isAutoRead()) {
                // stop accept new connections for 1 second to allow the channel to recover
                // See https://github.com/netty/netty/issues/1328
                config.setAutoRead(false);
                ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
            }
            // still let the exceptionCaught event flow through the pipeline to give the user
            // a chance to do something with it
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public ServerBootstrap clone() {
        return new ServerBootstrap(this);
    }

    /**
     * 返回已配置的 {@link EventLoopGroup}，该 EventLoopGroup 将用于子通道，如果未配置则返回null。
     * <p>
     * 注意: 此方法已弃用,使用 {@link #config()} 方法
     * <p>
     * 这里返回的是 childGroup,即: 用于处理每一个已建立连接发生的I/O读写事件
     * <p>
     * Return the configured {@link EventLoopGroup} which will be used for the child channels or {@code null}
     * if non is configured yet.
     *
     * @deprecated Use {@link #config()} instead.
     */
    @Deprecated
    public EventLoopGroup childGroup() {
        return childGroup;
    }

    final ChannelHandler childHandler() {
        return childHandler;
    }

    final Map<ChannelOption<?>, Object> childOptions() {
        synchronized (childOptions) {
            return copiedMap(childOptions);
        }
    }

    final Map<AttributeKey<?>, Object> childAttrs() {
        return copiedMap(childAttrs);
    }

    @Override
    public final ServerBootstrapConfig config() {
        return config;
    }
}
