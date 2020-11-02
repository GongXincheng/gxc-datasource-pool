package gxc.datasource.pool.config;

import gxc.datasource.pool.model.ConfigConstant;
import gxc.datasource.pool.util.DbUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author GongXincheng
 * @date 2020/11/2 14:17
 */
public class DataSourceConfigReader {

    public static DataSourceEntity getConfig() throws IOException {
        Properties prop = new Properties();
        prop.load(DbUtils.class.getClassLoader().getResourceAsStream(ConfigConstant.CONFIG_FILE_NAME));
        String url = prop.getProperty(ConfigConstant.CONFIG_URL);
        String userName = prop.getProperty(ConfigConstant.CONFIG_USERNAME);
        String pwd = prop.getProperty(ConfigConstant.CONFIG_PASSWORD);
        String poolSizeStr = prop.getProperty(ConfigConstant.CONFIG_POOL_SIZE);
        String maxWaitTimeStr = prop.getProperty(ConfigConstant.CONFIG_MAX_WAIT_TIME);
        Integer poolSize = StringUtils.isBlank(poolSizeStr) ? null : Integer.valueOf(poolSizeStr);
        Long maxWaitTime = StringUtils.isBlank(maxWaitTimeStr) ? null : Long.valueOf(maxWaitTimeStr);
        return new DataSourceEntity(url, userName, pwd, poolSize, maxWaitTime);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataSourceEntity {

        private String url;
        private String username;
        private String password;
        private Integer poolSize;
        private Long maxWaitTime;

    }
}



