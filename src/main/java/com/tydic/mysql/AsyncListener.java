package com.tydic.mysql;

import com.mysql.jdbc.AsyncUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by shihailong on 2017/9/21.
 */
public abstract class AsyncListener<T> extends ChannelInboundHandlerAdapter {
    private static final Log logger = LogFactory.getLog(AsyncListener.class);
    protected boolean isEOFDeprecated;
    private boolean inResultSetStream = false;
    private int columnCount;
    protected AsyncSocketChannel channel;
    private DefaultPromise<T> promise;
    private EventLoop eventLoop;
    private static final String TMP_LISTENER_NAME = "TMP_LISTENER";

    protected AsyncListener() {

    }

    public ChannelFuture register(AsyncSocketChannel asyncSocketChannel) {
        this.channel = asyncSocketChannel;
        if(!channel.isRegistered()) {
            this.isEOFDeprecated = channel.getIO().isEOFDeprecated();
            /**
             * 必须要注册之后，对pipeline进行添加，否则会导致采用channel原有的eventloop进行添加。会产生延迟，导致消息无法被接收。
             */
            return this.eventLoop.register(channel).addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) {
                    channel.pipeline().addLast(TMP_LISTENER_NAME, AsyncListener.this);
                }
            });
        }else{
            throw new RuntimeException("the channel is register on " + channel.eventLoop().threadProperties().name());
        }
    }

    public ChannelFuture deregister() {
        if (channel.isRegistered()) {
            channel.pipeline().remove(TMP_LISTENER_NAME);
            return channel.deregister();
        }else{
            throw new RuntimeException("the channel is not registered");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (ctx.isRemoved()) {
            ReferenceCountUtil.release(msg);
        }
        synchronized (this.channel.getConnectionMutex()) {
            if (msg instanceof ByteBuf) {
                ByteBuf byteBuf = (ByteBuf) msg;
                try {
                    channelRead(ctx, byteBuf);
                } finally {
                    byteBuf.release();
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }

    public void channelRead(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        if (inResultSetStream) {
            channelReadResultSet(ctx, byteBuf);
            return;
        }
        int type = byteBuf.getByte(4) & 0xFF;
        switch (type) {
            case 0:
                channelReadOKPacket(ctx, byteBuf);
                break;
            case 0xFE:
                channelReadEOFPacket(ctx, byteBuf);
                break;
            case 0xFF:
                channelReadErrorPacket(ctx, byteBuf);
                break;
            case 0xFC:
                type = (byteBuf.getByte(5) & 0xFF) | ((byteBuf.getByte(6) & 0xFF) << 8);
            default:
                inResultSetStream = true;
                columnCount = type;
                channelReadResultHeadPacket(ctx, byteBuf);
                break;
        }
    }

    private void channelReadResultSet(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        try {
            if (columnCount > 0) {
                //column define
                channelReadResultColumnsPacket(ctx, byteBuf);
            } else {
                if (columnCount == 0 && (!isEOFDeprecated)) {
                    if (isEOF(byteBuf)) {
                        channelReadEOFPacket(ctx, byteBuf);
                    } else {
                        throw new RuntimeException("为什么不是EOF包?");
                    }
                } else {
                    if (isEOF(byteBuf)) {
                        channelReadEOFPacket(ctx, byteBuf);
                    } else {
                        //row packet
                        channelReadResultRowDataPacket(ctx, byteBuf);
                    }
                }
            }
        } finally {
            --columnCount;
        }
    }

    private static boolean isEOF(ByteBuf byteBuf) {
        return ((byteBuf.getByte(4) & 0xFF) == 0xFE);
    }

    protected void channelReadResultHeadPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        channelReadResultSetPacket(ctx, byteBuf);
    }

    protected void channelReadResultColumnsPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        channelReadResultSetPacket(ctx, byteBuf);
    }

    protected void channelReadResultRowDataPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        channelReadResultSetPacket(ctx, byteBuf);
    }

    protected void channelReadResultSetPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadErrorPacket(ChannelHandlerContext ctx, ByteBuf error) {
        try {
            AsyncUtils.checkErrorPacket(channel.getIO(), error);
        } catch (SQLException e) {
            logger.error(e);
            setFailure(e);
        }
    }

    protected void channelReadEOFPacket(ChannelHandlerContext ctx, ByteBuf eof) {
        setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadOKPacket(ChannelHandlerContext ctx, ByteBuf ok) {
        setFailure(new IOException("非预期的报文"));
    }

    public Promise<T> getFuture() {
        if (promise == null) {
            promise = new DefaultPromise<>(getEventLoop());
        }
        return promise;
    }

    public final void setEventLoop(EventLoop eventLoop) {
        this.eventLoop = eventLoop;
    }

    private EventLoop getEventLoop() {
        return eventLoop;
    }

    protected final Promise<T> setFailure(final Throwable cause) {
        deregister().addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                promise.setFailure(cause);
            }
        });
        return promise;
    }

    protected final Promise<T> setSuccess(final T result) {
        deregister().addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                promise.setSuccess(result);
            }
        });
        return promise;
    }
}
