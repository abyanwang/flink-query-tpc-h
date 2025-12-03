package com.furui.processor;

import com.furui.constant.Status;
import com.furui.domain.LineItem;
import com.furui.domain.Orders;
import com.furui.domain.RealTimeResult;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class LineItemProcessorV2 extends KeyedCoProcessFunction<Integer, Orders, LineItem, RealTimeResult> {
    private ValueState<Orders> orderInfoState;
    private ValueState<Double> totalRevenueState;

    @Override
    public void open(Configuration parameters) {
        orderInfoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("order_info", Orders.class)
        );
        totalRevenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("total_revenue", Types.DOUBLE)
        );

    }

    @Override
    public void processElement1(Orders orders, KeyedCoProcessFunction<Integer, Orders, LineItem, RealTimeResult>.Context context, Collector<RealTimeResult> collector) throws Exception {
        int orderkey = orders.getO_orderkey();
        orderInfoState.update(orders);
//                        if (order.getStatus() == Status.DELETE) {
//                            System.out.println("DELETE"+orderkey);
//                        }

        if (totalRevenueState.value() != null) {
            double currentTotal = totalRevenueState.value();
            RealTimeResult result = new RealTimeResult(true, orderkey,
                    orders.getO_orderdate(),
                    orders.getO_shippriority(),
                    currentTotal);

            /**
             * 这里处理的时候，删除不清零缓存中的数据
             */
            if (Status.DELETE == orders.getStatus()) {
                result.setRevenue(0.0);
                result.setValid(false);
//                                System.out.print("DELETE");
            }
            collector.collect(result);
        }
    }

    @Override
    public void processElement2(LineItem lineItem, KeyedCoProcessFunction<Integer, Orders, LineItem, RealTimeResult>.Context context, Collector<RealTimeResult> collector) throws Exception {
        int orderkey = lineItem.getL_orderkey();

//                        if (orderkey == 5998051) {
//                            System.out.println(orderkey);
//                            System.out.println(orderInfoState.get(orderkey));
//                        }

        // 当前收入
        double itemRevenue = lineItem.getL_extendedprice() * (1 - lineItem.getL_discount());

        double currentTotal = totalRevenueState.value() == null ? 0.0 : totalRevenueState.value();
        if (Status.INSERT == lineItem.getStatus()) {
            currentTotal += itemRevenue;
        } else {
            currentTotal -= itemRevenue;
        }

        /**
         * 这里扣成负数也不清零，后面可以加回来
         */
        totalRevenueState.update(currentTotal);

        // 如果暂时没有 等订单来了触发
        if (orderInfoState.value() != null) {
            Orders order = orderInfoState.value();
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

            collector.collect(result);
        }
    }
}
