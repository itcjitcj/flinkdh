package com.itcj.FlinkSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table mySource(inputtime string,ts as cast(current_time as timestamp(3))" +
                ",watermark for ts as ts - interval '0' second ) " +
                " WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'flink'," +
                "  'properties.bootstrap.servers' = '192.168.197.130:9092'," +
                "  'properties.group.id' = 'test_group'," +
                "  'format' = 'json'" +
                ")");
//        tableEnv.executeSql("select * from mySource").print();

        tableEnv.executeSql("create table  mySink(window_start timestamp(3),window_end timestamp(3),value1 bigint) with ('connector'='print')");
//
        tableEnv.executeSql("insert into mySink select window_start,window_end,count(1) as value1 from " +
                "table(" +
                "tumble(table mySource,descriptor(ts) ,interval '10' second) " +
                ") " +
                "  group by window_start,window_end ");

        env.execute("FlinkSqlWindow");

    }
}
