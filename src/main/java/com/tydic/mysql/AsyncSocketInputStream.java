package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Created by shihailong on 2017/9/30.
 */
public final class AsyncSocketInputStream extends InputStream {
    private final AsyncSocketChannel channel;
    private final InputStream rawInputStream;
    private InputStream mock;

    AsyncSocketInputStream(AsyncSocketChannel channel, InputStream mock) {
        this.channel = channel;
        rawInputStream = new NoBlockingInputStream();
        this.mock = mock;
    }

    private class NoBlockingInputStream extends InputStream {
        SocketChannel sc = channel.javaChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(16384);
        ByteBuf byteBuf = Unpooled.EMPTY_BUFFER;
        Selector selector = channel.getSelector();

        @Override
        public int read() throws IOException {
            checkAvailable();
            return byteBuf.readByte();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            checkAvailable();
            int bytes = Math.min(len, byteBuf.readableBytes());
            byteBuf.readBytes(b, off, bytes);
            return bytes;
        }


        @Override
        public int available() {
            return byteBuf.readableBytes();
        }

        private void checkAvailable() throws IOException {
            if (available() <= 0) {
                tryRead();
            }
        }

        private void tryRead() throws IOException {
            buffer.clear();
            int read = sc.read(buffer);
            if (read > 0) {
                buffer.flip();
                byteBuf = Unpooled.wrappedBuffer(buffer);
                return;
            }
            SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ);
            long timeOut = 30000;
            try {
                do {
                    if (!sc.isOpen()) {
                        throw new ClosedChannelException();
                    }
                    long var9 = System.currentTimeMillis();
                    int var11 = selector.select(timeOut);
                    if (var11 > 0 && selectionKey.isReadable() && sc.read(buffer) != 0) {
                        buffer.flip();
                        byteBuf = Unpooled.wrappedBuffer(buffer);
                        return;
                    }
                    timeOut -= System.currentTimeMillis() - var9;
                } while (timeOut > 0L);
            } finally {
                selector.selectedKeys().remove(selectionKey);
            }
            throw new SocketTimeoutException();
        }
    }

    private InputStream delegate() {
        return channel.isRegistered() ? mock : rawInputStream;
    }

    @Override
    public int read() throws IOException {
        return delegate().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate().read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate().read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate().skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate().available();
    }

    @Override
    public void close() throws IOException {
        delegate().close();
    }

    @Override
    public void mark(int readlimit) {
        delegate().mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        delegate().reset();
    }

    @Override
    public boolean markSupported() {
        return delegate().markSupported();
    }
}
