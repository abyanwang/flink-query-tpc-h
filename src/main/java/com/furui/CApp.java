package com.furui;


import com.furui.domain.*;
import com.furui.general.LineItemAggregationProcess;
import com.furui.general.LineItemProcessFunction;
import com.furui.general.OrderProcessFunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ./dbgen -s 5 -f
 * cd /usr/local/opt/apache-flink@1/libexec/bin
 * ./start-cluster.sh
 * ./flink run -c com.furui.App /Users/free/Projects/flink-query-tpc-h/target/flink-query-tpc-h-1.0-SNAPSHOT.jar
 *
 * /usr/local/opt/apache-flink@1/libexec/conf
 * ./stop-cluster.sh
 */
public class CApp {

    private static final String SEGMENT = "BUILDING";
    private static final String DATE = "1995-03-15";

    private static List<String> CUSTOMER_DATA = new ArrayList<>();
    private static List<String> ORDER_DATA = new ArrayList<>();

    private static void loadFileToMemory(String filePath, List<String> dataList) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                dataList.add(line);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

//        loadFileToMemory("/Users/free/Projects/ipdata/customer.tbl", CUSTOMER_DATA);

        long start = System.currentTimeMillis();
        FileSource<String> customerFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdatabig/customer.tbl")        // 文件路径
                )
                .build();

        FileSource<String> orderFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdatabig/orders.tbl")        // 文件路径
                )
                .build();

        FileSource<String> lineFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdatabig/lineitem.tbl")        // 文件路径
                )
                .build();

        // 2. 从FileSource创建数据流，解析并过滤

        DataStream<Msg<Customer>> customerStream = env.fromSource(customerFileSource, WatermarkStrategy.noWatermarks(),
                "CustomerSource").map(Customer::convert).filter(c -> SEGMENT.equals(c.getData().getC_mktsegment()));



        DataStream<Msg<Orders>> orderStream = env.fromSource(
                orderFileSource,
                WatermarkStrategy.noWatermarks(),
                "OrderFileSource"
        ).map(Orders::convert).filter(o -> DATE.compareTo(o.getData().getO_orderdate()) > 0); // 过滤目标市场细分

        DataStream<Msg<Orders>> filteredOrders = customerStream.keyBy(customerMsg -> customerMsg.getData().getC_custkey())
                .connect(orderStream.keyBy(ordersMsg -> ordersMsg.getData().getO_custkey()))
                .process(new OrderProcessFunction());

        DataStream<Msg<LineItem>> lineitemStream = env.fromSource(
                lineFileSource,
                WatermarkStrategy.noWatermarks(), // 批处理场景无需水印
                "lineFileSource"
        ).map(LineItem::convert).filter(l -> DATE.compareTo(l.getData().getL_shipdate()) < 0);
//        lineitemStream.filter(i -> i.getData().getL_orderkey() == 47525).print();

        DataStream<Msg<RealTimeResult>> filterLineStream = filteredOrders.keyBy(o -> o.getData().getO_orderkey())
                        .connect(lineitemStream.keyBy(lineItemMsg -> lineItemMsg.getData().getL_orderkey()))
                                .process(new LineItemProcessFunction());

        DataStream<RealTimeResult> aggResult = filterLineStream.
                keyBy(realTimeResultMsg -> realTimeResultMsg.getData().getL_orderkey()).
                process(new LineItemAggregationProcess());


//        aggResult.print();

        String localOutputPath = "/Users/free/Projects/ipdata/flink_output/join_result";

        // 构建FileSink
        FileSink<String > fileSink = FileSink
                // 1. 使用Flink内置的SimpleStringEncoder（解决getBytes()问题）
                .forRowFormat(new Path(localOutputPath),
                        new SimpleStringEncoder<String>(StandardCharsets.UTF_8.name()) // 字符串编码为字节
                )
                // 2. 滚动策略（避免小文件）
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(128 * 1024 * 1024) // 128MB
                                .build()
                )
                .withBucketCheckInterval(1000)
                .build();
//        realTimeStream.print();
        aggResult.map(RealTimeResult::toString).sinkTo(fileSink).name("write");

//        aggResult.filter(i -> i.getL_orderkey() == 47525L).print();
        env.execute("Customer FileSource Demo");
//        Thread.sleep(10000);


        System.out.println(System.currentTimeMillis()-start);
    }
}
