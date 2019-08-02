package com.wulouzi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;
import java.util.Iterator;

public class IterativeStreamJob {
    public static void main(String[] args) throws Exception {

        //输入一组数据，我们对他们分别进行减1操作，直到等于0为止

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //0. 生成0到100的数列
        DataStream<Long> input = env.generateSequence(0, 100);

        //1. 基于输入流构建IterativeStream（定义迭代头）
        IterativeStream<Long> itStream = input.iterate();

        //2. 定义迭代逻辑（map function等）
        DataStream<Long> minusOne = itStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });

        //3. 定义反馈流逻辑（从迭代过的流中过滤出符合条件的元素组成的部分流反馈给迭代头进行重复计算的逻辑）
        DataStream<Long> greaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });

        //4. 调用IterativeStream的closeWith方法可以关闭一个迭代（即定义迭代尾）
        itStream.closeWith(greaterThanZero);

        //5. 定义”终止迭代“的逻辑（符合条件的元素将被分发给下游而不用进行下一次迭代）
        DataStream<Long> equalZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value == 0;
            }
        });
        //equalZero.print();

        Iterator<Long> myOutPut = DataStreamUtils.collect(equalZero);

        env.execute("IterativeStreamJob");

    }
}
