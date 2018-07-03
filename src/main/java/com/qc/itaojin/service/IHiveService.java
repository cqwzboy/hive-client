package com.qc.itaojin.service;

/**
 * @desc Hive通用服务类
 * @author fuqinqin
 * @date 2018-07-03
 */
public interface IHiveService {

    /**
     * @desc 初始化Hive，如果schema不存在就新建，table不存在也新建
     * @param schema 数据库实例
     * @param table 表
     * @param sql hiveSQL
     * @return boolean true-成功 false-失败
     * */
    boolean init(String schema, String table, String sql);

}
