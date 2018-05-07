package com.tydic.mysql;

import com.mysql.jdbc.MysqlIO;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shihailong on 2017/9/21.
 */
public final class AsyncSocketChannel extends NioSocketChannel {
    private MysqlIO io;
    private volatile Object connectionMutex;
    private AsyncSocket asyncSocket;

    private AsyncSocketInputStream asyncSocketInputStream;
    private AsyncSocketOutputStream asyncSocketOutputStream;
    private static byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private InputStream mockInputStream = new ByteArrayInputStream(OK);

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
            asyncSocketOutputStream = new AsyncSocketOutputStream(this, mockInputStream);
        }
        return asyncSocketOutputStream;
    }

    public InputStream getInputStream() {
        if (asyncSocketInputStream == null) {
            asyncSocketInputStream = new AsyncSocketInputStream(this, mockInputStream);
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
}
