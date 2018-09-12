package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public final class AsyncSocketOutputStream
        extends OutputStream {
    private final AsyncSocketChannel channel;
    private final AsyncSocketInputStream inputStream;
    private final Selector selector;
    private final SocketChannel socketChannel;

    AsyncSocketOutputStream(AsyncSocketChannel channel) {
        this.channel = channel;
        inputStream = (AsyncSocketInputStream) channel.getInputStream();
        selector = channel.getSelector();
        socketChannel = channel.javaChannel();
    }

    @Override
    public void write(int b) throws IOException {
        syncWrite(Unpooled.wrappedBuffer(new byte[]{(byte)b}));
    }

    @Override
    public void write(byte[] b) throws IOException {
        syncWrite(Unpooled.wrappedBuffer(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        syncWrite(Unpooled.wrappedBuffer(b, off, len));
    }
    private void waitWriteable(long timeOut) throws IOException {
        SelectionKey selectionKey = socketChannel.register(selector, SelectionKey.OP_WRITE);
        try {
            do {
                if (!socketChannel.isOpen()) {
                    throw new ClosedChannelException();
                }
                long var9 = System.currentTimeMillis();
                int var11 = selector.select(timeOut);
                if (var11 > 0 && selectionKey.isWritable()) {
                    return;
                }
                timeOut -= System.currentTimeMillis() - var9;
            } while (timeOut > 0L);
        } finally {
            selector.selectedKeys().remove(selectionKey);
        }
        throw new SocketTimeoutException();
    }
    private void syncWrite(ByteBuf byteBuf) throws IOException {
        channel.log("WRITE", byteBuf);
        ByteBuffer[] nioBuffers = byteBuf.nioBuffers();
        int nioBufferCnt = byteBuf.nioBufferCount();
        long expectedWrittenBytes = byteBuf.readableBytes();
        SocketChannel ch = channel.javaChannel();
        // Always us nioBuffers() to workaround data-corruption.
        // See https://github.com/netty/netty/issues/2761
        long maxWriteTimeout = System.currentTimeMillis() + 60000L;
        while (expectedWrittenBytes > 0) {
            switch (nioBufferCnt) {
                case 0:
                    return;
                case 1:
                    // Only one ByteBuf so use non-gathering write
                    ByteBuffer nioBuffer = nioBuffers[0];
                    for (int i = channel.config().getWriteSpinCount() - 1; i >= 0; i--) {
                        final int localWrittenBytes = ch.write(nioBuffer);
                        if (localWrittenBytes == 0) {
                            waitWriteable(maxWriteTimeout - System.currentTimeMillis());
                            continue;
                        }
                        expectedWrittenBytes -= localWrittenBytes;
                        if (expectedWrittenBytes == 0) {
                            break;
                        }
                    }
                    break;
                default:
                    for (int i = channel.config().getWriteSpinCount() - 1; i >= 0; i--) {
                        final long localWrittenBytes = ch.write(nioBuffers, 0, nioBufferCnt);
                        if (localWrittenBytes == 0) {
                            waitWriteable(maxWriteTimeout - System.currentTimeMillis());
                            continue;
                        }
                        expectedWrittenBytes -= localWrittenBytes;
                        if (expectedWrittenBytes == 0) {
                            break;
                        }
                    }
                    break;
            }
        }
        if(channel.isRegistered()){
            inputStream.switchToMock();
        }
    }
}
