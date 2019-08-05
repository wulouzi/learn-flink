package com.wulouzi.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestIntervalJoin {

    private static class Order {
        private String orderId;
        private long createTime;

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public Order(String orderId, long createTime) {
            this.orderId = orderId;
            this.createTime = createTime;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }
    }

    private static class Totalpay {
        private String totalpayId;
        private String orderId;
        private long totalPrice;
        private long createTime;

        public Totalpay(String totalpayId, String orderId, long totalPrice, long createTime) {
            this.totalpayId = totalpayId;
            this.orderId = orderId;
            this.totalPrice = totalPrice;
            this.createTime = createTime;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        public String getTotalpayId() {
            return totalpayId;
        }

        public void setTotalpayId(String totalpayId) {
            this.totalpayId = totalpayId;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public long getTotalPrice() {
            return totalPrice;
        }

        public void setTotalPrice(long totalPrice) {
            this.totalPrice = totalPrice;
        }
    }

    public static final Order[] ORDERS = new Order[]{
            new Order("1", System.currentTimeMillis()),
            new Order("2", System.currentTimeMillis()),
            new Order("3", System.currentTimeMillis())
    };

    public static final Totalpay[] TOTALPAYS = new Totalpay[]{
            new Totalpay("1", "1", 100, System.currentTimeMillis()),
            new Totalpay("2", "2", 101, System.currentTimeMillis()),
            new Totalpay("3", "3", 102, System.currentTimeMillis()),
            new Totalpay("4", "4", 103, System.currentTimeMillis())
    };

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置时间类型为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //datasource1, 分配时间
        DataStream<Order> inputOrder = env.fromElements(ORDERS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
            @Override
            public long extractAscendingTimestamp(Order element) {
                return element.createTime;
            }
        });

        //datasource2, 分配时间
        DataStream<Totalpay> inputTotalpay = env.fromElements(TOTALPAYS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Totalpay>() {
            @Override
            public long extractAscendingTimestamp(Totalpay element) {
                return element.createTime;
            }
        });

        KeyedStream<Order, String> keyedStreamOrder = inputOrder.keyBy(new KeySelector<Order, String>() {
            @Override
            public String getKey(Order value) throws Exception {
                return value.orderId;
            }
        });

        KeyedStream<Totalpay, String> keyedStreamTotalpay = inputTotalpay.keyBy(new KeySelector<Totalpay, String>() {
            @Override
            public String getKey(Totalpay value) throws Exception {
                return value.orderId;
            }
        });

        //key1==key2 && e1.timestamp + lowerBound < e2.timestamp < e1.timestamp + upperBound
        keyedStreamOrder.intervalJoin(keyedStreamTotalpay)
                .between(Time.milliseconds(-2), Time.milliseconds(2))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Order, Totalpay, Object>() {

                    @Override
                    public void processElement(Order left, Totalpay right, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(new Tuple5<>(left.orderId,left.createTime,right.totalpayId,right.totalPrice,right.createTime));
                    }
                }).print();

        env.execute();
    }
}

