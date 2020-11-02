package gxc.datasource.pool.util;

import gxc.datasource.pool.config.DataSourceConfigReader;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author GongXincheng
 * @date 2020/11/1 16:27
 */
public class DbUtils {

    /**
     * 正在使用的Connection
     */
    private static final Set<Connection> ACTIVE_CONNECTION_POOL;

    /**
     * 空闲Connection
     */
    private static final Set<Connection> IDLE_CONNECTION_POOL;

    /**
     * 默认连接池大小.
     */
    private static final Integer DEFAULT_POOL_SIZE = 20;

    /**
     * 默认等待时间
     */
    private static final Long DEFAULT_MAX_WAIT_TIME = 2000L;

    /**
     * 客户指定最大等待时长(毫秒)
     */
    private static Long CUSTOMER_MAX_WAIT_TIME;


    static {
        ACTIVE_CONNECTION_POOL = new HashSet<>();
        IDLE_CONNECTION_POOL = new HashSet<>();
        try {
            // 读取配置.
            DataSourceConfigReader.DataSourceEntity configEntity = DataSourceConfigReader.getConfig();
            // 从配置中获取线程池数量
            Integer customerPoolSize = configEntity.getPoolSize();
            // 获取最大等待时间
            CUSTOMER_MAX_WAIT_TIME = configEntity.getMaxWaitTime();

            // 如果没有获取到配置则设置默认为10
            for (int i = 0; i < (Objects.isNull(customerPoolSize) ? DEFAULT_POOL_SIZE : customerPoolSize); i++) {
                Connection newConnection = getNewConnection(configEntity.getUrl(), configEntity.getUsername(), configEntity.getPassword());
                IDLE_CONNECTION_POOL.add(newConnection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取新的连接
     */
    private static Connection getNewConnection(String url, String username, String password) throws Exception {
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 获取连接.
     */
    public synchronized static Connection getConnection() throws Exception {
        // 判断空闲连接池中是否有可以连接
        Connection connection = getConnectionFromIdlePool();
        if (Objects.nonNull(connection)) {
            return connection;
        }

        // 如果连接池为空，等待   -----
        while (CollectionUtils.isEmpty(IDLE_CONNECTION_POOL)) {
            DbUtils.class.wait(Objects.isNull(CUSTOMER_MAX_WAIT_TIME) ? DEFAULT_MAX_WAIT_TIME : CUSTOMER_MAX_WAIT_TIME);
        }

        // 如果等待超时会抛出异常
        // 如果被唤醒,说明有可用连接
        connection = getConnectionFromIdlePool();
        if (Objects.isNull(connection)) {
            throw new RuntimeException("Get db connection error from idle connection pool after thread wait!");
        }
        return connection;
    }

    /**
     * 从空闲线程池中获取连接，如果获取不到为 null
     */
    private synchronized static Connection getConnectionFromIdlePool() {
        if (CollectionUtils.isEmpty(IDLE_CONNECTION_POOL)) {
            return null;
        }

        // 获取连接
        Connection connection = IDLE_CONNECTION_POOL.stream().findFirst()
                .orElseThrow(() -> new RuntimeException("Get db connection error from idle connection pool !"));

        // 从【空闲连接池】中删除
        IDLE_CONNECTION_POOL.remove(connection);

        // 将其移动到【活动连接池】中
        ACTIVE_CONNECTION_POOL.add(connection);

        return connection;
    }


    /**
     * 关闭指定连接.
     *
     * @param connection Connection
     * @return 是否关闭成功
     */
    public synchronized static boolean close(Connection connection) {
        // 判断该连接是否在【活动连接池】中
        if (!ACTIVE_CONNECTION_POOL.contains(connection)) {
            return false;
        }

        // 从【活动线程池】中移除
        ACTIVE_CONNECTION_POOL.remove(connection);

        // 放回【空闲线程池】中
        IDLE_CONNECTION_POOL.add(connection);

        // 唤醒等待线程
        DbUtils.class.notify();
        return true;
    }

    /**
     * 批量关闭指定连接.
     */
    public synchronized static void closeAll() throws Exception {
        for (Connection connection : ACTIVE_CONNECTION_POOL) {
            connection.close();
        }
        for (Connection connection : IDLE_CONNECTION_POOL) {
            connection.close();
        }
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                try {
                    Connection connection = DbUtils.getConnection();
                    System.out.println(Thread.currentThread().getName() + "——" + connection);
                    DbUtils.close(connection);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "thread_" + i).start();
        }
        Thread.sleep(5000);
        DbUtils.closeAll();
    }

}
