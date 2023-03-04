package com.itcj.FlinkSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlWindowJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table mySource(id0 string,number0 bigint,ts as cast(current_time as timestamp(3))" +
                ",watermark for ts as ts - interval '0' second ) " +
                " WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'flink'," +
                "  'properties.bootstrap.servers' = '192.168.197.130:9092'," +
                "  'properties.group.id' = 'test_group'," +
                "  'format' = 'json'" +
                ")");

        tableEnv.executeSql("create table mySource1(id1 string,number1 bigint,ts as cast(current_time as timestamp(3))" +
                ",watermark for ts as ts - interval '0' second ) " +
                " WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'flink1'," +
                "  'properties.bootstrap.servers' = '192.168.197.130:9092'," +
                "  'properties.group.id' = 'test_group'," +
                "  'format' = 'json'" +
                ")");
//        tableEnv.executeSql("select * from mySource").print();

        tableEnv.executeSql("create table  mySink(id0 string,id1 string,number bigint) with ('connector'='print')");
//
//        tableEnv.executeSql("insert into mySink select a.window_start,a.window_end,a.id0 as id,number0+number1 as value1 from " +
//                "(select window_start,window_end,id0,sum(number0) as number0 from  table(" +
//                "tumble(table mySource,descriptor(ts) ,interval '10' second) " +
//                ") group by window_start,window_end,id0 )  a " +
//                "left join " +
//                "(select window_start,window_end,id1,sum(number1) as number1 from  table(" +
//                "tumble(table mySource1,descriptor(ts) ,interval '10' second) " +
//                ") group by window_start,window_end,id1 ) b " +
//                "ON a.id0=b.id1 and a.window_start=b.window_start and a.window_end=b.window_end " +
//                "   ");

        //通过两张表按照id分组分别计算总和然后合并总和 作为id的最终总和
        tableEnv.executeSql("insert into mySink select a.id0,b.id1,a.number0+b.number1  from " +
                "(select id0,sum(number0) as number0 from mySource group by id0)  a " +
                "left join " +
                "(select id1,sum(number1) as number1 from mySource1 group by id1) b " +
                "on a.id0=b.id1 ");


        env.execute("FlinkSqlWindow");

    }
}
