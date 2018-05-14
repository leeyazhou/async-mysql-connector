package com.tydic.mysql;

import com.mysql.jdbc.*;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by shihailong on 2017/9/22.
 */
public class AsyncStatementInterceptor implements StatementInterceptorV2 {
    private static final Log LOGGER = LogFactory.getLog(AsyncStatementInterceptor.class);

    private AsyncSocketChannel channel;
    private MySQLConnection mySQLConnection;
    private Statement interceptStatement;
    private AsyncListener listener;
    private EventLoop eventLoop;
    private MySQLConnection activeMySQLConnection;

    public void init(Connection conn, Properties props) throws SQLException {
        this.mySQLConnection = conn.unwrap(MySQLConnection.class);
    }

    private void setCurrentActiveMySQLConnection() {
        activeMySQLConnection = mySQLConnection.getActiveMySQLConnection();
    }

    private void setCurrentAsyncSocketChannel() throws SQLException {
        MysqlIO io = activeMySQLConnection.getIO();
        AsyncSocket asyncSocket = (AsyncSocket) io.mysqlConnection;
        channel = asyncSocket.getAsyncSocketChannel();
        channel.setIO(io);
        channel.setConnectionMutex(this);
    }

    private static AsyncStatementInterceptor getAsyncStatementInterceptor(java.sql.Statement statement) throws SQLException {
        java.sql.Connection connection = statement.getConnection();
        MySQLConnection mySQLConnection = connection.unwrap(MySQLConnection.class);
        for (StatementInterceptorV2 statementInterceptorV2 : mySQLConnection.getStatementInterceptorsInstances()) {
            if (statementInterceptorV2 instanceof AsyncStatementInterceptor) {
                return (AsyncStatementInterceptor) statementInterceptorV2;
            }
        }
        throw new RuntimeException("not found AsyncStatementInterceptor!");
    }

    public static <T> Future<T> intercept(EventLoop eventLoop, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        interceptor.eventLoop = eventLoop;
        return intercept(interceptor, statement, listener);
    }

    public static <T> Future<T> intercept(java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        AsyncStatementInterceptor interceptor = getAsyncStatementInterceptor(statement);
        return intercept(interceptor, statement, listener);
    }

    private static <T> Future<T> intercept(AsyncStatementInterceptor interceptor, java.sql.Statement statement, AsyncListener<T> listener) throws SQLException {
        interceptor.interceptStatement = statement.unwrap(Statement.class);
        interceptor.setCurrentActiveMySQLConnection();
        interceptor.setCurrentAsyncSocketChannel();
        EventLoop eventLoop = interceptor.eventLoop;
        if (eventLoop == null) {
            eventLoop = interceptor.channel.eventLoop();
        }
        listener.setEventLoop(eventLoop);
        interceptor.listener = listener;
        return listener.getFuture();
    }

    public ResultSetInternalMethods preProcess(String sql, Statement interceptedStatement, Connection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("preProcess " + sql);
        }
        if (this.channel == null || this.listener == null || !isInterceptedStatement(interceptedStatement)) {
            return null;
        }
        assert this.eventLoop.inEventLoop();
        listener.register(channel).syncUninterruptibly();
        this.interceptStatement = null;
        this.listener = null;
        this.eventLoop = null;
        return null;
    }

    private boolean isInterceptedStatement(Statement interceptedStatement) {
        if(interceptStatement == null || interceptedStatement == null){
            return false;
        }
        // proxy 时,怎么判断是否拦截的问题
        return  interceptStatement == interceptedStatement || interceptedStatement.toString().equals(interceptStatement.toString());
    }

    public boolean executeTopLevelOnly() {
        return true;
    }

    public void destroy() {

    }

    public ResultSetInternalMethods postProcess(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet, Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("postProcess " + sql);
        }
        return null;
    }
}
