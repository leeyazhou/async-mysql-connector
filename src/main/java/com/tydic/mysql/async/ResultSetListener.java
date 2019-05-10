package com.tydic.mysql.async;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.AsyncUtils;
import com.mysql.jdbc.StatementImpl;
import com.tydic.mysql.AsyncListener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by shihailong on 2017/9/21.
 */
public class ResultSetListener extends AsyncListener<ResultSet> {
    private Statement statement;
    private ByteBuf result = Unpooled.buffer();


    public ResultSetListener(Statement statement) {
        this.statement = statement;
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
                setSuccess(AsyncUtils.build((StatementImpl) statement, new ByteBufInputStream(result)));
            } catch (SQLException e) {
                setFailure(e);
            }
        }
    }
}
