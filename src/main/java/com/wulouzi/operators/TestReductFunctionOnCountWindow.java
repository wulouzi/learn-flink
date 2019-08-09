package com.wulouzi.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestReductFunctionOnCountWindow {
    private static final Tuple4<String, String, String, Integer>[] SCORE = new Tuple4[]{
            Tuple4.of("Class1", "张三", "数学", 100),
            Tuple4.of("Class1", "张三", "语文", 90),
            Tuple4.of("Class1", "张三", "英语", 95),
            Tuple4.of("Class2", "李四", "数学", 100),
            Tuple4.of("Class2", "李四", "英语", 100),
            Tuple4.of("Class2", "李四", "语文", 100)
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<String, String, String, Integer>> input = env.fromElements(SCORE);

        KeyedStream<Tuple4<String, String, String, Integer>, Tuple2<String, String>> keyedStream = input.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1);
            }
        });

        DataStream<Tuple4<String, String, String, Integer>> out = keyedStream.countWindow(3).reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1, Tuple4<String, String, String, Integer> value2) throws Exception {
                return new Tuple4<>(value1.f0, value1.f1, "总分", value1.f3 + value2.f3);
            }
        });

        out.print();

        env.execute();

    }
}
