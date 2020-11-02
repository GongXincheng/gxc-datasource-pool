package sam.datasource.pool;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * mysql connection pool
 *
 * @author chusen
 * @date 2020/11/1 4:22 下午
 */
public class ConnectionPool {

    private final int DEFAULT_MAX_CONNECTION = 50;

    private final int DEFAULT_MIN_CONNECTION = 10;

    private final int DEFAULT_TIME_OUT = 3000;


    private int maxConnectionSize = DEFAULT_MAX_CONNECTION;


    private final ScheduledExecutorService executors = new ScheduledThreadPoolExecutor(1, r -> {
        Thread thread = new Thread(r, "valid-check");
        thread.setDaemon(true);
        return thread;
    });


    private final String URL = "jdbc:mysql://localhost:3306/test";
    private final String USER_NAME = "root";
    private final String USER_PASSWORD = "root";

    private int minConnectionSize = DEFAULT_MIN_CONNECTION;

    private int poolSize;

    private BlockingQueue<Connection> pool;
    private final Lock lock = new ReentrantLock();

    ConnectionPool() {
        // init
        pool = new ArrayBlockingQueue<>(getMinConnectionSize());
        // get the connection
        doGetConnection(getMinConnectionSize());
        this.poolSize = getMinConnectionSize();

        executors.scheduleWithFixedDelay(() -> {
            Iterator<Connection> iterator = pool.iterator();
            try {
                while (iterator.hasNext()) {
                    Connection connection = iterator.next();
                    if (!connection.isValid(DEFAULT_TIME_OUT / 1000)) {
                        System.err.println("the connection " + connection + "is valid will be remove from the pool!");
                        iterator.remove();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 5000, 10000, TimeUnit.MILLISECONDS);
    }

    private void doGetConnection(int count) {
        for (int i = 0; i < count; i++) {
            try {
                Connection connection = DriverManager.getConnection(URL, USER_NAME, USER_PASSWORD);
                pool.offer(connection);
            } catch (Exception e) {
                throw new RuntimeException("can't connection the mysql server!", e);
            }
        }
    }

    private static volatile ConnectionPool INSTANCE;

    public static ConnectionPool getInstance() {
        if (INSTANCE == null) {
            synchronized (ConnectionPool.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConnectionPool();
                }
            }
        }
        return INSTANCE;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getMaxConnectionSize() {
        return maxConnectionSize;
    }

    public void setMaxConnectionSize(int maxConnectionSize) {
        if (maxConnectionSize < 1) {
            throw new IllegalArgumentException("the maxConnection size must >= 1!");
        }
        this.maxConnectionSize = maxConnectionSize;
    }

    public int getMinConnectionSize() {
        return minConnectionSize;
    }

    public void setMinConnectionSize(int minConnectionSize) {
        if (minConnectionSize < 0) {
            throw new IllegalArgumentException("the minConnectionSize must > 0!");
        }
        this.minConnectionSize = minConnectionSize;
    }


    public int getTotalActiveConnection() {
        return pool.size();
    }


    public void releaseConnection(Connection connection) throws SQLException {
        if (!connection.isValid(DEFAULT_TIME_OUT / 1000)) {
            throw new SQLException("the connection:" + connection + " may closed, please check it!");
        }
        try {
            if (!pool.offer(connection, DEFAULT_TIME_OUT, TimeUnit.MILLISECONDS)) {
                throw new SQLException("release connection fail!");
            }
        } catch (InterruptedException e) {
            throw new SQLException("release connection fail!", e);
        }
    }

    /**
     * get the db connection
     *
     * @param timeout
     * @param timeUnit
     * @return
     */
    public Connection getConnection(long timeout, TimeUnit timeUnit) throws SQLException, InterruptedException {
        Connection connection = pool.poll();
        if (connection == null && getMaxConnectionSize() == getPoolSize() && (connection = pool.poll(timeout, timeUnit)) == null) {
            throw new SQLException(Thread.currentThread().getName() + "get the connection exception.");
        }
        if (connection != null) {
            return connection;
        }

        lock.lock();
        try {
            growPoolSize(getMaxConnectionSize() - getPoolSize());
            connection = pool.poll(timeout, timeUnit);
        } finally {
            lock.unlock();
        }
        if (connection == null) {
            throw new SQLException(Thread.currentThread().getName() + "==get the connection exception.");
        }
        return connection;
    }

    /**
     *
     */
    private void growPoolSize(int count) {
        if (getTotalActiveConnection() == 0 && count > 0) {
            this.pool = new ArrayBlockingQueue<>(getMaxConnectionSize());
            doGetConnection(count);
            this.poolSize = getMaxConnectionSize();
        }
    }


}
