package com.wulouzi.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;


public class TestAggregateFunctionOnCountWindow {
    private static final Tuple4<String, String, String, Long>[] SCORE = new Tuple4[]{
            Tuple4.of("Class1", "张三", "数学", 100L),
            Tuple4.of("Class1", "张三", "语文", 90L),
            Tuple4.of("Class1", "张三", "英语", 95L),
            Tuple4.of("Class1", "王五", "数学", 100L),
            Tuple4.of("Class1", "王五", "语文", 90L),
            Tuple4.of("Class1", "王五", "英语", 95L),
            Tuple4.of("Class2", "李四", "数学", 100L),
            Tuple4.of("Class2", "李四", "英语", 100L),
            Tuple4.of("Class2", "李四", "语文", 100L),
            Tuple4.of("Class2", "孙六", "数学", 100L),
            Tuple4.of("Class2", "孙六", "语文", 90L),
            Tuple4.of("Class2", "孙六", "英语", 95L)
    };

    private static class AverageAggregate
            implements AggregateFunction<Tuple4<String, String, String, Long>, ArrayList<Tuple4<String, String, Long, Long>>, ArrayList<Tuple3<String, String, Double>>> {


        @Override
        public ArrayList<Tuple4<String, String, Long, Long>> createAccumulator() {

            ArrayList<Tuple4<String, String, Long, Long>> accumulator = new ArrayList<>();

            accumulator.add(new Tuple4<>("Class1", "语文", 0L, 0L));
            accumulator.add(new Tuple4<>("Class1", "数学", 0L, 0L));
            accumulator.add(new Tuple4<>("Class1", "英语", 0L, 0L));
            accumulator.add(new Tuple4<>("Class2", "语文", 0L, 0L));
            accumulator.add(new Tuple4<>("Class2", "数学", 0L, 0L));
            accumulator.add(new Tuple4<>("Class2", "英语", 0L, 0L));

            return accumulator;
        }

        @Override
        public ArrayList<Tuple4<String, String, Long, Long>> add(Tuple4<String, String, String, Long> value, ArrayList<Tuple4<String, String, Long, Long>> accumulator) {

            ArrayList<Tuple4<String, String, Long, Long>> accumulatorAdd = new ArrayList<>();

            for (int i = 0; i < accumulator.size(); i++) {
                if (value.f0.equals(accumulator.get(i).f0) && value.f2.equals(accumulator.get(i).f1)) {
                    accumulatorAdd.add(new Tuple4<>(value.f0, value.f2, value.f3 + accumulator.get(i).f2, accumulator.get(i).f3 + 1L));
                } else {
                    accumulatorAdd.add(accumulator.get(i));
                }
            }
            return accumulatorAdd;
        }

        @Override
        public ArrayList<Tuple3<String, String, Double>> getResult(ArrayList<Tuple4<String, String, Long, Long>> accumulator) {
            ArrayList<Tuple3<String, String, Double>> result = new ArrayList<>();
            for (int i = 0; i < accumulator.size(); i++) {
                result.add(new Tuple3<>(accumulator.get(i).f0, accumulator.get(i).f1, ((double) accumulator.get(i).f2) / accumulator.get(i).f3));
            }
            return result;
        }

        @Override
        public ArrayList<Tuple4<String, String, Long, Long>> merge(ArrayList<Tuple4<String, String, Long, Long>> a, ArrayList<Tuple4<String, String, Long, Long>> b) {
            ArrayList<Tuple4<String, String, Long, Long>> mergeAccumulator = new ArrayList<>();
            for (int i = 0; i < a.size(); i++) {
                for (int j = 0; j < b.size(); j++) {
                    if (a.get(i).f0.equals(b.get(j).f0) && a.get(i).f1.equals(b.get(j).f1)) {
                        mergeAccumulator.add(new Tuple4<>(a.get(i).f0, a.get(i).f1, a.get(i).f2 + b.get(j).f2, a.get(i).f3 + b.get(j).f3));
                    }
                }
            }

            return mergeAccumulator;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<String, String, String, Long>> input = env.fromElements(SCORE);

        KeyedStream<Tuple4<String, String, String, Long>, Tuple2<String, String>> keyedStream = input.keyBy(new KeySelector<Tuple4<String, String, String, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple4<String, String, String, Long> value) throws Exception {
                return new Tuple2<>(value.f0, value.f2);
            }
        });

        DataStream<ArrayList<Tuple3<String, String, Double>>> out = keyedStream.countWindow(2).aggregate(new AverageAggregate());

        out.print();

        env.execute();

    }

}
