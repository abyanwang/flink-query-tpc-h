package com.furui.utils;

import com.furui.domain.Customer;
import com.furui.domain.Orders;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CheckDistinctApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        FileSource<String> orderFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdata/orders.tbl")        // 文件路径
                )
                .build();


//        env.fromSource(
//                orderFileSource,
//                WatermarkStrategy.noWatermarks(),
//                "OrderFileSource"
//        ).map(Orders::convert) // 字符串转 Orders 实体（确保 convert 方法正确）
//                .keyBy(Orders::getO_orderkey) // 3. 按主键分组（关键：状态与主键绑定）
//                .process(new CountAndFilterDuplicate()) // 4. 统计次数并过滤重复
//                .print("重复数据"); // 输出结果

        env.execute("Filter Duplicate Primary Key");

    }

    static class CountAndFilterDuplicate extends KeyedProcessFunction<Integer, Orders, Orders> {

        // 状态：存储当前主键（orderId）的出现次数（Integer 类型）
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化状态描述符（名称+类型）
            ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>(
                    "orderId_count", // 状态名称（自定义）
                    Types.INT // 状态值类型
            );
            // 从上下文获取状态（KeyedStream 才能获取）
            countState = getRuntimeContext().getState(countDesc);
        }

        @Override
        public void processElement(Orders order, Context ctx, Collector<Orders> out) throws Exception {
            // 1. 获取当前主键的已有次数（首次为 null）
            Integer currentCount = countState.value();

            // 2. 更新次数：首次出现设为1，否则+1
            if (currentCount == null) {
                countState.update(1);
            } else {

                // 3. 次数>1 时，输出该重复数据
                out.collect(order);
            }
        }
    }
}
