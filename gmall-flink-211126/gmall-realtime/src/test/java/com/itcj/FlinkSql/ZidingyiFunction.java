package com.itcj.FlinkSql;

import com.itcj.FlinkSql.util.SubstringFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ZidingyiFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table mySource(id0 string,number0 bigint,ts as cast(current_time as timestamp(3))" +
                ",watermark for ts as ts - interval '0' second ) " +
                " WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'flink'," +
                "  'properties.bootstrap.servers' = '192.168.197.130:9092'," +
                "  'properties.group.id' = 'test_group'," +
                "  'format' = 'json'," +
                " 'scan.startup.mode' = 'group-offsets' "+
                ")");

        tableEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

        tableEnv.executeSql("select SubstringFunction(id0,0,1) from mySource").print();
        env.execute();
    }
}

