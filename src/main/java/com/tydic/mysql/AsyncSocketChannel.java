package com.tydic.mysql;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.MysqlIO;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by shihailong on 2017/9/21.
 */
public final class AsyncSocketChannel extends NioSocketChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSocketChannel.class);

    private MysqlIO io;
    private volatile Object connectionMutex;
    private AsyncSocket asyncSocket;

    private AsyncSocketInputStream asyncSocketInputStream;
    private AsyncSocketOutputStream asyncSocketOutputStream;

    private Selector selector;

    public AsyncSocketChannel() {
        super();
        try {
            selector = this.javaChannel().provider().openSelector();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public Selector getSelector() {
        return selector;
    }

    public SocketChannel javaChannel() {
        return super.javaChannel();
    }

    @Override
    protected void doClose() throws Exception {
        super.doClose();
        this.selector.close();
    }

    public OutputStream getOutputStream() {
        if(asyncSocketOutputStream == null){
            asyncSocketOutputStream = new AsyncSocketOutputStream(this);
        }
        return asyncSocketOutputStream;
    }

    public InputStream getInputStream() {
        if (asyncSocketInputStream == null) {
            asyncSocketInputStream = new AsyncSocketInputStream(this);
        }
        return asyncSocketInputStream;
    }

    public void setIO(MysqlIO io) {
        this.io = io;
    }

    public MysqlIO getIO() {
        return io;
    }

    public Object getConnectionMutex() {
        return connectionMutex;
    }

    public void setConnectionMutex(Object connectionMutex) {
        this.connectionMutex = connectionMutex;
    }

    public AsyncSocket getAsyncSocket() {
        return asyncSocket;
    }

    public void setAsyncSocket(AsyncSocket asyncSocket) {
        this.asyncSocket = asyncSocket;
    }
    void log(String eventName, ByteBuf msg){
        if(LOGGER.isInfoEnabled()){
            String chStr = this.toString();
            int length = msg.readableBytes();
            StringBuilder buf;
            if (length == 0) {
                buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 4);
                buf.append(chStr).append(' ').append(eventName).append(": 0B");
            } else {
                int rows = length / 16 + (length % 15 == 0? 0 : 1) + 4;
                buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 2 + 10 + 1 + 2 + rows * 80);

                buf.append(chStr).append(' ').append(eventName).append(": ").append(length).append('B').append(NEWLINE);
                appendPrettyHexDump(buf, msg);
            }
            LOGGER.info(buf.toString());
        }
    }
}
