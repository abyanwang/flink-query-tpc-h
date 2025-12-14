package com.furui.general;

import com.furui.domain.Msg;
import com.furui.constant.Status;
import com.furui.domain.Customer;
import com.furui.domain.Orders;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


@Slf4j
public class OrderProcessFunction extends CoProcessFunction<Msg<Customer>, Msg<Orders>, Msg<Orders>> {

    private ValueState<Set<Orders>> alive;

    private ValueState<Integer> holder;

    @Override
    public void open(Configuration parameters) throws Exception {
        alive = getRuntimeContext().getState(new ValueStateDescriptor<Set<Orders>>("OrderProcessFunction.alive", TypeInformation.of(new TypeHint<Set<Orders>>() {
        })));
        holder = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("OrderProcessFunction.holder", TypeInformation.of(new TypeHint<Integer>() {
        })));
    }

    @Override
    public void processElement1(Msg<Customer> customerMsg, CoProcessFunction<Msg<Customer>, Msg<Orders>, Msg<Orders>>.Context context, Collector<Msg<Orders>> collector) throws Exception {
        Customer customer = customerMsg.getData();
        if (null == customer) {
            return;
        }
        if (null == alive.value()) {
            alive.update(new HashSet<>());
        }
        if (Status.INSERT == customerMsg.getStatus()) {
            holder.update(holder.value() == null ? 1 : holder.value()+1);
            if (holder.value() == 1) { // 避免多次，理论没有
                Set<Orders> current = alive.value();
                if (!CollectionUtils.isEmpty(current)) {
                    current.forEach(i -> {
                        collector.collect(collect(i, Status.INSERT));
                    });
//                    current.clear();
                }
            }
        } else if(Status.DELETE == customerMsg.getStatus()) {
            holder.update(holder.value() == null ? 0 : holder.value()-1);
            if (holder.value() == 0) {
                Set<Orders> current = alive.value();
                if (!CollectionUtils.isEmpty(current)) {
                    current.forEach(i -> {
                        collector.collect(collect(i, Status.DELETE));
                    });
                }
                log.error("delete {}", customer.getC_custkey());
            }
        }

    }

    @Override
    public void processElement2(Msg<Orders> ordersMsg, CoProcessFunction<Msg<Customer>, Msg<Orders>, Msg<Orders>>.Context context, Collector<Msg<Orders>> collector) throws Exception {
        Orders orders = ordersMsg.getData();
        if (null == orders) {
            return;
        }

        if (null == alive.value()) {
            alive.update(new HashSet<>());
        }

        Set<Orders> current = alive.value();

        if (Status.INSERT == ordersMsg.getStatus()) {
            current.add(orders);
//            ordersMapState.put(orders.getO_custkey(), orders);
            if (null != holder.value() && holder.value() == 1) {
                collector.collect(collect(orders, Status.INSERT));
            }
        } else if (Status.DELETE == ordersMsg.getStatus()) {
            if (holder.value() != null && holder.value() == 1 && current.contains(orders)) { //减少当前这个且得是live的
                collector.collect(collect(orders, Status.DELETE));
            }
            log.error("delete order : {}, {}", orders.getO_custkey(), orders.getO_orderkey());
            current.remove(orders);
        }

    }

    private Msg<Orders> collect(Orders orders, Status status) {
        if (Status.INSERT == status) {
            return Msg.<Orders>builder().data(orders).status(Status.INSERT).build();
        } else if (Status.DELETE == status) {
            return Msg.<Orders>builder().data(orders).status(Status.DELETE).build();
        }
        return null;
    }
}
