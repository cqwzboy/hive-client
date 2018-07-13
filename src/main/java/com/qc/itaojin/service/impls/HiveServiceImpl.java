package com.qc.itaojin.service.impls;

import com.qc.itaojin.common.HiveConstants;
import com.qc.itaojin.dao.HiveBaseDao;
import com.qc.itaojin.service.BaseServiceImpl;
import com.qc.itaojin.service.IHiveService;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * @desc Hive服务类
 * @author fuqinqin
 * @date 2018-07-02
 */
@Data
@Slf4j
public class HiveServiceImpl extends BaseServiceImpl implements IHiveService {

    private HiveBaseDao hiveBaseDao;

    @Override
    public boolean init(String schema, String table, String sql) {
        if(StringUtils.isBlank(schema) || StringUtils.isBlank(table) || StringUtils.isBlank(sql)){
            return false;
        }

        // step 1. 查询该schema是否存在，不存在则新建
        hiveBaseDao.setSchema(HiveConstants.DEFAULT_SCHEMA);
        if(!hiveBaseDao.schemaExists(schema)){
            log.info("Hive 不存在实例{}，新建之", schema);

            // step 2. 创建schema
            if(!hiveBaseDao.execute("create database "+schema)){
                log.info("Hive 创建数据库实例{} 失败", schema);
                return false;
            }
        }

        // step 3. 判断新建的表格在Hive中是否已经存在，若不存在则新建
        hiveBaseDao.setSchema(schema);
        Set<String> oldTables = hiveBaseDao.findAllTables();
        if(oldTables.contains(table)){
            log.info("schema={}中已经存在table={}", schema, table);
            return true;
        }

        // step 4. 建表
        if(!hiveBaseDao.execute(sql)){
            log.info("Hive 建表 {}.{} 失败", schema, table);
            return false;
        }

        log.info("初始化Hive数据库 {}.{} 成功！", schema, table);

        return true;
    }
}
