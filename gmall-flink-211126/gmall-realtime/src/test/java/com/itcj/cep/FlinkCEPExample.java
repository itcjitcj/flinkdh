package com.itcj.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class FlinkCEPExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("192.168.197.130", 9999);

        DataStream<Tuple2<Integer, Long>> inputStream = stream
                .map(new MapFunction<String, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return Tuple2.of(Integer.valueOf(arr[0]), Long.valueOf(arr[1]));
                    }
                });

        Pattern<Tuple2<Integer, Long>, ?> pattern = Pattern
                .<Tuple2<Integer, Long>>begin("start")
                .where(new IterativeCondition<Tuple2<Integer, Long>>() {
                    @Override
                    public boolean filter(Tuple2<Integer, Long> value, Context<Tuple2<Integer, Long>> context) throws Exception {
                        return value.f0 == 1;
                    }
                })
                .followedBy("middle")
                .where(new IterativeCondition<Tuple2<Integer, Long>>() {
                    @Override
                    public boolean filter(Tuple2<Integer, Long> value, Context<Tuple2<Integer, Long>> context) throws Exception {
                        return value.f0 == 2;
                    }
                })
                .within(Time.seconds(10));

        DataStream<Tuple2<Integer, Long>> result = CEP.pattern(inputStream, pattern).inProcessingTime()
                .select(new PatternSelectFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> select(Map<String, List<Tuple2<Integer, Long>>> pattern) throws Exception {
                        return pattern.get("start").get(0);
                    }
                });

        result.print();

        env.execute("Flink CEP Example");
    }
}
