package com.furui.processor;

import com.furui.constant.Status;
import com.furui.domain.LineItem;
import com.furui.domain.Orders;
import com.furui.domain.RealTimeResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class LineItemProcessor extends KeyedCoProcessFunction<Integer, Orders, LineItem, RealTimeResult> {

    // 状态1：缓存订单基础信息（orderkey -> 订单信息）
    private MapState<Integer, Orders> orderInfoState;
    // 状态2：存储当前分组的累计收入（orderkey -> 累计值）
    private MapState<Integer, Double> totalRevenueState;

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
//                        if (order.getStatus() == Status.DELETE) {
//                            System.out.println("DELETE"+orderkey);
//                        }

        if (totalRevenueState.contains(orderkey)) {
            double currentTotal = totalRevenueState.get(orderkey);
            RealTimeResult result = new RealTimeResult(true, orderkey,
                    order.getO_orderdate(),
                    order.getO_shippriority(),
                    currentTotal);

            /**
             * 这里处理的时候，删除不清零缓存中的数据
             */
            if (Status.DELETE == order.getStatus()) {
                result.setRevenue(0.0);
                result.setValid(false);
//                                System.out.print("DELETE");
            }
            out.collect(result);
        }
    }


    @Override
    public void processElement2(LineItem lineitem, Context ctx, Collector<RealTimeResult> out) throws Exception {
        int orderkey = lineitem.getL_orderkey();

//                        if (orderkey == 5998051) {
//                            System.out.println(orderkey);
//                            System.out.println(orderInfoState.get(orderkey));
//                        }

        // 当前收入
        double itemRevenue = lineitem.getL_extendedprice() * (1 - lineitem.getL_discount());

        double currentTotal = totalRevenueState.get(orderkey) == null ? 0.0 : totalRevenueState.get(orderkey);
        if (Status.INSERT == lineitem.getStatus()) {
            currentTotal += itemRevenue;
        } else {
            currentTotal -= itemRevenue;
        }

        /**
         * 这里扣成负数也不清零，后面可以加回来
         */
        totalRevenueState.put(orderkey, currentTotal);

        // 3. 关联订单属性
        // 如果暂时没有 等订单来了触发
        if (orderInfoState.contains(orderkey)) {
            Orders order = orderInfoState.get(orderkey);
            String orderdate =  order.getO_orderdate();
            int shippriority = order.getO_shippriority();

            RealTimeResult result = new RealTimeResult(true,
                    orderkey,
                    orderdate,
                    shippriority,
                    currentTotal);

            if (currentTotal <= 0 || Status.DELETE == order.getStatus()) {
                result.setValid(false);
                //total 这里设置成负的也输出可以按照需求修改.
            }

            out.collect(result);
        }
    }
}
