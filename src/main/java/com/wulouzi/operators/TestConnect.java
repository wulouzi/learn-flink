package com.wulouzi.operators;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TestConnect {
    private static final String[] WORDS = new String[]{
            "Apache Flink is an open source platform for distributed stream and batch data processing.",
            "Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. ",
            "Flink builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization."
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> inputLong = env.generateSequence(0, 100);

        DataStream<String> inputString = env.fromElements(WORDS);

        ConnectedStreams<Long, String> connectedStreams = inputLong.connect(inputString);

        DataStream<String> dataStream = connectedStreams.flatMap(new CoFlatMapFunction<Long, String, String>() {
            @Override
            public void flatMap1(Long value, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                String[] tokens = value.split("\\W+");
                for (String token : tokens) {
                    out.collect(token);
                }

            }
        });

        dataStream.print();

        env.execute();
    }
}
