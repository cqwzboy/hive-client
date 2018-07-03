package com.qc.itaojin.autoconfiguration;

import com.qc.itaojin.config.ItaojinHiveConfig;
import com.qc.itaojin.dao.HiveBaseDao;
import com.qc.itaojin.service.IHiveService;
import com.qc.itaojin.service.impls.HiveServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by fuqinqin on 2018/7/3.
 */
@Configuration
@EnableConfigurationProperties(ItaojinHiveConfig.class)
@ConditionalOnClass({HiveBaseDao.class,IHiveService.class})
@ConditionalOnProperty(prefix = "itaojin", value = "enabled", matchIfMissing = true)
public class ItaojinHiveAutoConfiguration {

    @Autowired
    private ItaojinHiveConfig hiveConfig;

    @Bean
    @ConditionalOnMissingBean
    public HiveBaseDao hiveBaseDao(){
        HiveBaseDao hiveBaseDao = new HiveBaseDao();
        hiveBaseDao.setDriver(hiveConfig.getDriver());
        hiveBaseDao.setPreUrl(hiveConfig.getPreUrl());
        return hiveBaseDao;
    }

    @Bean
    @ConditionalOnMissingBean
    public IHiveService hiveService(){
        IHiveService hiveService = new HiveServiceImpl();
        ((HiveServiceImpl) hiveService).setHiveBaseDao(hiveBaseDao());
        return hiveService;
    }

}
