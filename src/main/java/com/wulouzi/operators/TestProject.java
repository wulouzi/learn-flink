package com.wulouzi.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestProject {
    private static final Tuple4<String, Integer, Long, Long>[] ORDERS = new Tuple4[]{
            Tuple4.of("1",123, System.currentTimeMillis(),100),
            Tuple4.of("2",123, System.currentTimeMillis(),200),
            Tuple4.of("3",123, System.currentTimeMillis(),300),
            Tuple4.of("4",123, System.currentTimeMillis(),400)
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<String,Integer,Long,Long>> input = env.fromElements(ORDERS);

        DataStream<Tuple3<String,Long,Long>> projectStream = input.project(0,2,3);

        projectStream.print();

        env.execute();
    }
}
