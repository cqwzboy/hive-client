package com.qc.itaojin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @desc Hive的安全配置类
 * @author fuqiqnin
 * @date 2018-07-03
 */
@ConfigurationProperties(prefix = "itaojin.hive")
@Data
public class ItaojinHiveConfig {

    /**
     * 默认驱动
     * */
    private static final String DEFAULT_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private String driver = DEFAULT_DRIVER;;

    private String preUrl;

}
