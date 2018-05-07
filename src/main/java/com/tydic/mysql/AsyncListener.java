package com.tydic.mysql;

import com.mysql.jdbc.AsyncUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
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
    protected DefaultPromise<T> promise;
    protected boolean init = false;

    public AsyncListener(AsyncSocketChannel asyncSocketChannel) {
        super();
        init(asyncSocketChannel, asyncSocketChannel.eventLoop());
    }

    protected AsyncListener() {

    }

    public void init(AsyncSocketChannel asyncSocketChannel, EventLoop eventLoop) {
        if (init) {
            return;
        }
        this.channel = asyncSocketChannel;
        this.isEOFDeprecated = channel.getIO().isEOFDeprecated();
        this.promise = new DefaultPromise<>(eventLoop);
        init = true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(ctx.isRemoved()){
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
        promise.setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadErrorPacket(ChannelHandlerContext ctx, ByteBuf error) {
        try{
            AsyncUtils.checkErrorPacket(channel.getIO(), error);
        }catch (SQLException e){
            logger.error(e);
            promise.setFailure(e);
        }
    }

    protected void channelReadEOFPacket(ChannelHandlerContext ctx, ByteBuf eof) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    protected void channelReadOKPacket(ChannelHandlerContext ctx, ByteBuf ok) {
        promise.setFailure(new IOException("非预期的报文"));
    }

    public Future<T> getFuture() {
        return promise;
    }
}
