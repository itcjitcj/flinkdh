package com.itcj.cep;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.itcj.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlinkCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> input = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("flink", "cep"));
        SingleOutputStreamOperator<JSONObject> JsonDs = input.map(JSON::parseObject);
        KeyedStream<JSONObject, String> keyedStream = JsonDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(json -> json.getString("id"));
//        keyedStream.print();
//        input.print();
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                return value.getInteger("number") == 1;
                            }
                        }
                )
//                .next("middle").where(
//                        new SimpleCondition<JSONObject>() {
//                            @Override
//                            public boolean filter(JSONObject value) throws Exception {
//                                return value.getInteger("number") == 2;
//                            }
//                        }
//                )
//                .followedBy("end").where(
//                        new SimpleCondition<JSONObject>() {
//                            @Override
//                            public boolean filter(JSONObject value) throws Exception {
//                                return value.getInteger("number") == 3;
//                            }
//                        }
//                )
//                .times(2)
                .oneOrMore()
                .within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern).inProcessingTime();
        OutputTag<String> outputTag = new OutputTag<String>("timeout", Types.STRING);
        SingleOutputStreamOperator<String> cepstream = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });
        cepstream.print("cep:");
        cepstream.getSideOutput(outputTag).print("timeout:");

//        SingleOutputStreamOperator<String> process = patternStream.process(new PatternProcessFunction<String, String>() {
//            @Override
//            public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {
//                collector.collect(map.get("start").get(0));
//            }
//        });
//        process.print();
        env.execute("cep");

    }
}
