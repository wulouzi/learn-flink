package com.wulouzi.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestFlatMap {

    private static final String[] WORDS = new String[]{
            "Apache Flink is an open source platform for distributed stream and batch data processing.",
            "Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. ",
            "Flink builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization."
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements(WORDS);

        DataStream<String> wordStream = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");

                for(String token : tokens){
                    if(token.length() > 0){
                        out.collect(token);
                    }
                }
            }
        });

        wordStream.print();

        env.execute();

    }
}
