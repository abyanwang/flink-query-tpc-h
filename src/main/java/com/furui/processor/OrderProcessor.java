package com.furui.processor;

import com.furui.constant.Status;
import com.furui.domain.Customer;
import com.furui.domain.Orders;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OrderProcessor extends BroadcastProcessFunction<Orders, Customer, Orders> {

    private MapStateDescriptor<Integer, Customer> customerStateDesc;

    public OrderProcessor(MapStateDescriptor<Integer, Customer> descriptor) {
        this.customerStateDesc = descriptor;
    }

    @Override
    public void processElement(Orders order, ReadOnlyContext ctx, Collector<Orders> out) throws Exception {
        ReadOnlyBroadcastState<Integer, Customer> customerState = ctx.getBroadcastState(this.customerStateDesc);
        // 如果客户存在且属于目标细分，保留订单
        if (customerState.contains(order.getO_custkey())) {
            Customer current = customerState.get(order.getO_custkey());
            if (Status.DELETE == current.getStatus()) {
                order.setStatus(Status.DELETE);
            }
            out.collect(order);
        }
    }

    @Override
    public void processBroadcastElement(Customer customer, Context ctx, Collector<Orders> out) throws Exception {
        BroadcastState<Integer, Customer> state = ctx.getBroadcastState(this.customerStateDesc);
        state.put(customer.getC_custkey(), customer);
    }
}
