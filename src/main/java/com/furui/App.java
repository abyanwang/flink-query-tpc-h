package com.furui;

import com.furui.constant.Status;
import com.furui.domain.Customer;
import com.furui.domain.LineItem;
import com.furui.domain.Orders;
import com.furui.domain.RealTimeResult;
import com.furui.processor.LineItemProcessor;
import com.furui.processor.LineItemProcessorV2;
import com.furui.processor.OrderProcessor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ./dbgen -s 5 -f
 * cd /usr/local/opt/apache-flink@1/libexec/bin
 * ./start-cluster.sh
 * ./flink run -c com.furui.App /Users/free/Projects/flink-query-tpc-h/target/flink-query-tpc-h-1.0-SNAPSHOT.jar
 *
 * /usr/local/opt/apache-flink@1/libexec/conf
 * ./stop-cluster.sh
 */
public class App {

    private static final String SEGMENT = "BUILDING"; // 市场细分（随机选择）
    private static final String DATE = "1995-03-15";  // 日期（1995-03-01至31之间）

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        long start = System.currentTimeMillis();
        FileSource<String> customerFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdata/customer.tbl")        // 文件路径
                )
                .build();

        FileSource<String> orderFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdata/orders.tbl")        // 文件路径
                )
                .build();

        FileSource<String> lineFileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(), // 按行读取文本
                        new Path("/Users/free/Projects/ipdata/lineitem.tbl")        // 文件路径
                )
                .build();

        // 2. 从FileSource创建数据流，解析并过滤
        DataStream<Customer> customerStream = env.fromSource(
                customerFileSource,
                WatermarkStrategy.noWatermarks(), // 批处理场景无需水印
                "CustomerFileSource"
        ).map(Customer::convert).filter(c -> SEGMENT.equals(c.getC_mktsegment())); // 过滤目标市场细分

        MapStateDescriptor<Integer, Customer> customerStateDesc = new MapStateDescriptor<>(
                "customerState",
                Types.INT,
                Types.POJO(Customer.class)
        );

        BroadcastStream<Customer> broadcastCustomer = customerStream.broadcast(customerStateDesc);


        DataStream<Orders> orderStream = env.fromSource(
                orderFileSource,
                WatermarkStrategy.noWatermarks(),
                "OrderFileSource"
        ).map(Orders::convert).filter(o -> DATE.compareTo(o.getO_orderdate()) > 0); // 过滤目标市场细分


        DataStream<Orders> filteredOrders = orderStream
                .connect(broadcastCustomer)
                .process(new OrderProcessor(customerStateDesc));

        DataStream<LineItem> lineitemStream = env.fromSource(
                lineFileSource,
                WatermarkStrategy.noWatermarks(), // 批处理场景无需水印
                "lineFileSource"
        ).map(LineItem::convert).filter(l -> DATE.compareTo(l.getL_shipdate()) < 0);

        DataStream<RealTimeResult> realTimeStream = filteredOrders
                .keyBy(Orders::getO_orderkey) // 按订单ID分组
                .connect(lineitemStream.keyBy(LineItem::getL_orderkey))
                .process(new LineItemProcessorV2());

        realTimeStream.filter(i -> !i.isValid()).print();

        env.execute("Customer FileSource Demo");
//        Thread.sleep(10000);

        System.out.println(System.currentTimeMillis()-start);
    }
}
