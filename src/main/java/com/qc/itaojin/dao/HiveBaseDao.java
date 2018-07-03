package com.qc.itaojin.dao;

import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @desc hive通用dao
 * @author fuqinqin
 * @date 2018-07-03
 */
@Data
public class HiveBaseDao {

    private String driver;

    private String preUrl;

    private ThreadLocal<String> localSchema = new ThreadLocal<>();

    public void setSchema(String schema){
        this.localSchema.set(schema);
    }

    private String getSchema(){
        return this.localSchema.get();
    }

    /**
     * 获取Hive的连接对象
     * */
    public Connection getConn(){
        Connection conn = null;

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(StringUtils.contact(preUrl, getSchema()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 执行Hive的SQL
     * */
    public boolean execute(String sql){
        if(StringUtils.isBlank(sql)){
            return false;
        }

        Connection conn = null;
        PreparedStatement pstat = null;

        try{
            conn = getConn();
            if(conn == null){
                return false;
            }

            pstat = conn.prepareStatement(sql);
            pstat.execute();

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat);
        }

        return false;
    }

    /**
     * 获取Hive中所有数据库实例集合
     * */
    public Set<String> findAllDataBases(){
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet res = null;

        try {
            Set<String> set = new HashSet<>();
            conn = getConn();
            pstat = conn.prepareStatement("show databases");
            res = pstat.executeQuery();
            if(res != null){
                while (res.next()){
                    set.add(res.getString(1));
                }
            }

            return set;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat, res);
        }

        return null;
    }

    /**
     * 获取Hive中所有数据库实例集合
     * */
    public Set<String> findAllTables(){
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet res = null;

        try {
            Set<String> set = new HashSet<>();
            conn = getConn();
            pstat = conn.prepareStatement("show tables in "+getSchema());
            res = pstat.executeQuery();
            if(res != null){
                while (res.next()){
                    set.add(res.getString(1));
                }
            }

            return set;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, pstat, res);
        }

        return null;
    }

    /**
     * 查询某个数据库实例是否已经存在
     * */
    public boolean schemaExists(String schema){
        if(StringUtils.isBlank(schema)){
            return false;
        }

        Set<String> schemas = findAllDataBases();
        if(CollectionUtils.isEmpty(schemas)){
            return false;
        }

        return schemas.contains(schema);
    }


    protected void close(Connection conn){
        close(conn, null, null);
    }

    protected void close(Connection conn, Statement state){
        close(conn, state, null);
    }

    protected void close(Connection conn, Statement state, ResultSet res){
        close(state, conn, res);
    }

    protected void close(Statement state,Connection conn, ResultSet... res){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }

        if(state != null){
            try {
                state.close();
            } catch (SQLException e) {
            }
        }

        if(res!=null && res.length>0){
            for (ResultSet re : res) {
                if(re != null){
                    try {
                        re.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }
}
