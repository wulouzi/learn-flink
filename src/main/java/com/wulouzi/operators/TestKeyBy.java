package com.wulouzi.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.stream.Stream;

public class TestKeyBy {
    private static final String[] WORDS = new String[]{
            "Apache Flink is an open source platform for distributed stream and batch data processing.",
            "Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. ",
            "Flink builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization."
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements(WORDS);

        DataStream<Tuple2<String,Integer>> wordStream = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");
                for(String token : tokens){
                    if(token.length() > 0){
                        out.collect(new Tuple2<>(token,1));
                    }
                }
            }
        });

        KeyedStream<Tuple2<String,Integer>,Tuple> keyedStream = wordStream.keyBy(0);

        keyedStream.min(1).print();

        env.execute();
    }
}
