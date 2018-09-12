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
    private static byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    private InputStream mock;

    AsyncSocketInputStream(AsyncSocketChannel channel) {
        this.channel = channel;
        rawInputStream = new NoBlockingInputStream();
        mock = new ByteArrayInputStream(OK);
        try {
            //noinspection ResultOfMethodCallIgnored
            mock.skip(OK.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void switchToMock() {
        try {
            mock.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        public int available() throws IOException {
            if (byteBuf.readableBytes() == 0) {
                fastRead();
            }
            return byteBuf.readableBytes();
        }

        private void checkAvailable() throws IOException {
            if (available() <= 0) {
                if (fastRead()) return;
                timeOutRead();
            }
        }

        private void timeOutRead() throws IOException {
            buffer.clear();
            SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_READ);
            long timeOut = 60000;
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
                        channel.log("READ", byteBuf);
                        return;
                    }
                    timeOut -= System.currentTimeMillis() - var9;
                } while (timeOut > 0L);
            } finally {
                selector.selectedKeys().remove(selectionKey);
            }
            throw new SocketTimeoutException();
        }

        private boolean fastRead() throws IOException {
            buffer.clear();
            int read = sc.read(buffer);
            if (read > 0) {
                buffer.flip();
                byteBuf = Unpooled.wrappedBuffer(buffer);
                channel.log("READ", byteBuf);
                return true;
            }
            return false;
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
        rawInputStream.close();
        mock.close();
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
