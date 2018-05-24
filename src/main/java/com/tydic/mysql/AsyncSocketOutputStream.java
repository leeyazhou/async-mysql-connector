package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;

public final class AsyncSocketOutputStream
        extends OutputStream {
    private final AsyncSocketChannel channel;
    private final AsyncSocketInputStream inputStream;

    AsyncSocketOutputStream(AsyncSocketChannel channel) {
        this.channel = channel;
        inputStream = (AsyncSocketInputStream) channel.getInputStream();

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
    private void syncWrite(ByteBuf byteBuf) throws IOException {
        channel.log("WRITE", byteBuf);
        ByteBuffer[] nioBuffers = byteBuf.nioBuffers();
        int nioBufferCnt = byteBuf.nioBufferCount();
        long expectedWrittenBytes = byteBuf.readableBytes();
        SocketChannel ch = channel.javaChannel();
        // Always us nioBuffers() to workaround data-corruption.
        // See https://github.com/netty/netty/issues/2761
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
                            throw new RuntimeException("write 0 bytes to " + channel.remoteAddress().toString());
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
                            throw new RuntimeException("write 0 bytes to " + channel.remoteAddress().toString());
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
