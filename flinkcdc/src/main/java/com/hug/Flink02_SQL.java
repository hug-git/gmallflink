package com.hug;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink02_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用SQL方式读取MySQL变化数据
        tableEnv.executeSql("create table trademark(tm_id string,tm_name string) with(" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'gmall2020'," +
                "'table-name' = 'base_trademark'" +
                ")");

        Table table = tableEnv.sqlQuery("select * from trademark");
        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
