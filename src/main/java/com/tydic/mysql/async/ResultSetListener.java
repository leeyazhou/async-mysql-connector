package com.tydic.mysql.async;

import com.mysql.jdbc.AsyncUtils;
import com.mysql.jdbc.StatementImpl;
import com.tydic.mysql.AsyncListener;
import com.tydic.mysql.AsyncSocketChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PipedOutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by shihailong on 2017/9/21.
 */
public class ResultSetListener extends AsyncListener<ResultSet> {
    private Statement statement;
    private ByteBuf result = Unpooled.buffer();


    public ResultSetListener(AsyncSocketChannel asyncSocketChannel, Statement statement) {
        super(asyncSocketChannel);
        this.statement = statement;
    }

    public ResultSetListener(Statement statement) {
        super();
        this.statement = statement;
    }

    @Override
    public void init(AsyncSocketChannel asyncSocketChannel, EventLoop eventLoop) {
        super.init(asyncSocketChannel, eventLoop);
    }

    @Override
    protected void channelReadResultSetPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        result = Unpooled.wrappedBuffer(result, byteBuf);
    }

    @Override
    protected void channelReadEOFPacket(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        result = Unpooled.wrappedBuffer(result, byteBuf);
        if (isEOFDeprecated) {
            try {
                promise.setSuccess(AsyncUtils.build((StatementImpl) statement, new ByteArrayInputStream(result.array())));
            } catch (SQLException e) {
                promise.setFailure(e);
            }
        }
    }
}
