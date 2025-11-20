package com.furui;

import com.furui.domain.Customer;
import com.furui.domain.LineItem;
import com.furui.domain.Orders;
import com.furui.domain.RealTimeResult;
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
        ).map(line -> {
            String[] fields = line.split("\\|");
            Customer customer = new Customer();
            customer.setC_custkey(Integer.parseInt(fields[0]));
            customer.setC_mktsegment(fields[6]);
            return customer;
        }).filter(c -> SEGMENT.equals(c.getC_mktsegment())); // 过滤目标市场细分

        MapStateDescriptor<Integer, Customer> customerStateDesc = new MapStateDescriptor<>(
                "customerState",
                Types.INT,
                Types.POJO(Customer.class)
        );

        BroadcastStream<Customer> broadcastCustomer = customerStream.broadcast(customerStateDesc);


        DataStream<Orders> orderStream = env.fromSource(
                orderFileSource,
                WatermarkStrategy.noWatermarks(), // 批处理场景无需水印
                "OrderFileSource"
        ).map(line -> {
            String[] fields = line.split("\\|");
            Orders orders = new Orders();
            orders.setO_orderkey(Integer.parseInt(fields[0]));
            orders.setO_custkey(Integer.parseInt(fields[1]));
            orders.setO_orderdate(fields[4]);
            orders.setO_shippriority(Integer.parseInt(fields[7]));
            return orders;
        }).filter(o -> DATE.compareTo(o.getO_orderdate()) > 0); // 过滤目标市场细分


        DataStream<Orders> filteredOrders = orderStream
                .connect(broadcastCustomer)
                .process(new BroadcastProcessFunction<Orders, Customer, Orders>() {
                    // 处理orders流元素：检查客户是否在广播的目标市场细分中
                    @Override
                    public void processElement(Orders order, ReadOnlyContext ctx, Collector<Orders> out) throws Exception {
                        ReadOnlyBroadcastState<Integer, Customer> customerState = ctx.getBroadcastState(customerStateDesc);
                        // 如果客户存在且属于目标细分，保留订单
                        if (customerState.contains(order.getO_custkey())) {
                            out.collect(order);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Customer customer, Context ctx, Collector<Orders> out) throws Exception {
                        BroadcastState<Integer, Customer> state = ctx.getBroadcastState(customerStateDesc);
                        state.put(customer.getC_custkey(), customer);
                    }
                });

        DataStream<LineItem> lineitemStream = env.fromSource(
                lineFileSource,
                WatermarkStrategy.noWatermarks(), // 批处理场景无需水印
                "lineFileSource"
        ).map(line -> {
            String[] fields = line.split("\\|");
            LineItem lineitem = new LineItem();
            lineitem.setL_orderkey(Integer.parseInt(fields[0]));
            lineitem.setL_extendedprice(Double.parseDouble(fields[5]));
            lineitem.setL_discount(Double.parseDouble(fields[6]));
            lineitem.setL_shipdate(fields[10]);
            return lineitem;
        }).filter(l -> DATE.compareTo(l.getL_shipdate()) < 0);

        DataStream<RealTimeResult> realTimeStream = filteredOrders
                .keyBy(Orders::getO_orderkey) // 按订单ID分组
                .connect(lineitemStream.keyBy(LineItem::getL_orderkey))
                .process(new KeyedCoProcessFunction<Integer, Orders, LineItem, RealTimeResult>() {
                    // 状态1：缓存订单基础信息（orderkey -> 订单信息）
                    private MapState<Integer, Orders> orderInfoState;
                    // 状态2：存储当前分组的累计收入（orderkey -> 累计值）
                    private MapState<Integer, Double> totalRevenueState;
                    // 状态3：存储当前分组已处理的订单项数量（orderkey -> 计数）
//                    private MapState<Long, Integer> itemCountState;

                    @Override
                    public void open(Configuration parameters) {
                        orderInfoState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("order_info", Integer.class, Orders.class)
                        );
                        totalRevenueState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("total_revenue", Types.INT, Types.DOUBLE)
                        );

                    }

                    @Override
                    public void processElement1(Orders order, Context ctx, Collector<RealTimeResult> out) throws Exception {
                        int orderkey = order.getO_orderkey();
                        orderInfoState.put(orderkey, order);

                        if (totalRevenueState.contains(orderkey)) {
                            double currentTotal = totalRevenueState.get(orderkey);
                            out.collect(new RealTimeResult(
                                    orderkey,
                                    order.getO_orderdate(),
                                    order.getO_shippriority(),
                                    currentTotal
                            ));
                        }
                    }


                    @Override
                    public void processElement2(LineItem lineitem, Context ctx, Collector<RealTimeResult> out) throws Exception {
                        int orderkey = lineitem.getL_orderkey();

                        // 1. 更新累计收入
                        double itemRevenue = lineitem.getL_extendedprice() * (1 - lineitem.getL_discount());

                        double currentTotal = totalRevenueState.get(orderkey) == null ? 0.0 : totalRevenueState.get(orderkey);
                        currentTotal += itemRevenue;
                        totalRevenueState.put(orderkey, currentTotal);

                        // 3. 关联订单属性（可能为null，若订单未到达）
                        if (orderInfoState.contains(orderkey)) {
                            Orders order = orderInfoState.get(orderkey);
                            String orderdate =  order.getO_orderdate();
                            int shippriority = order.getO_shippriority();

                            out.collect(new RealTimeResult(
                                    orderkey,
                                    orderdate,
                                    shippriority,
                                    currentTotal
                            ));
                        }
                    }
                });

        realTimeStream.print();

        env.execute("Customer FileSource Demo");

        System.out.println(System.currentTimeMillis()-start);
    }
}
