package com.tydic.mysql;

import com.mysql.jdbc.SocketFactory;
import com.tydic.mysql.async.MySQLBufferFrameDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncSocketFactory implements SocketFactory {
    private static final Log LOGGER = LogFactory.getLog(AsyncSocketFactory.class);
    static final String MY_SQL_BUFFER_FRAME_DECODER_NAME = "MY_SQL_BUFFER_FRAME_DECODER";

    private static int DEFAULT_EVENT_LOOP_THREADS = SystemPropertyUtil.getInt(
            "com.tydic.async-mysql.threads", 2);

    static final String EVENT_LOOP_KEY = "com.tydic.mysql.async.eventLoopGroup";

    private static final String DEFAULT_INBOUND_HANDLER = "DEFAULT_INBOUND_HANDLER";

    private static final String DEFAULT_OUTBOUND_HANDLER = "DEFAULT_OUTBOUND_HANDLER";

    public static final String DEFAULT_LOG_HANDLER = "DEFAULT_LOG_HANDLER";

    private Bootstrap nettyBootstrap;

    static {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    private void init() {
        nettyBootstrap = new Bootstrap();
        EventLoopGroup eventExecutors = AsyncCall.getEventLoopGroup();
        if (eventExecutors == null) {
            eventExecutors = new NioEventLoopGroup(DEFAULT_EVENT_LOOP_THREADS,
                    new DefaultThreadFactory("async-mysql"));
            AsyncCall.setEventLoopGroup(eventExecutors);
        }
        nettyBootstrap.group(eventExecutors).channel(AsyncSocketChannel.class);
        nettyBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        nettyBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        nettyBootstrap.option(ChannelOption.AUTO_READ, false);

        ChannelInitializer<AsyncSocketChannel> channelInitializer = getAsyncSocketChannelChannelInitializer();
        nettyBootstrap.handler(channelInitializer);
    }

    private static ChannelInitializer<AsyncSocketChannel> getAsyncSocketChannelChannelInitializer() {
        return new ChannelInitializer<AsyncSocketChannel>() {
            @Override
            protected void initChannel(final AsyncSocketChannel ch) {
                ch.pipeline().addLast(DEFAULT_LOG_HANDLER, new LoggingHandler(LogLevel.DEBUG));
                ch.pipeline().addLast(MY_SQL_BUFFER_FRAME_DECODER_NAME, new MySQLBufferFrameDecoder());
            }
        };
    }

    /**
     * The underlying TCP/IP socket to use
     */
    private Socket rawSocket = null;

    public Socket afterHandshake() {
        return rawSocket;
    }

    public Socket beforeHandshake() {
        return rawSocket;
    }

    public synchronized Socket connect(String host, int portNumber, Properties props) throws IOException {
        try {
            if (nettyBootstrap == null) {
                init();
            }
            AsyncSocketChannel channel = (AsyncSocketChannel) nettyBootstrap.connect(host, portNumber).sync().channel();
            channel.deregister().syncUninterruptibly();
            channel.config().setAutoRead(true);
            rawSocket = new AsyncSocket(channel);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return rawSocket;
    }
}
