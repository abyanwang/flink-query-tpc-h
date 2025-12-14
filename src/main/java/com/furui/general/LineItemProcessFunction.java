package com.furui.general;

import com.alibaba.fastjson.JSON;
import com.furui.constant.Status;
import com.furui.domain.*;
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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class LineItemProcessFunction extends CoProcessFunction<Msg<Orders>, Msg<LineItem>, Msg<RealTimeResult>> {

    private ValueState<Set<LineItem>> alive;

    private ValueState<Integer> holder;

    private ValueState<Orders> attribute;



    @Override
    public void open(Configuration parameters) throws Exception {

        alive = getRuntimeContext().getState(new ValueStateDescriptor<Set<LineItem>>("LineItemProcessFunction.alive",
                TypeInformation.of(new TypeHint<Set<LineItem>>() {
        })));
        holder = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("LineItemProcessFunction.holder",
                TypeInformation.of(new TypeHint<Integer>() {
        })));
        attribute = getRuntimeContext().getState(new ValueStateDescriptor<Orders>("LineItemProcessFunction.attribute",
                TypeInformation.of(new TypeHint<Orders>() {
        })));
    }

    @Override
    public void processElement1(Msg<Orders> ordersMsg, CoProcessFunction<Msg<Orders>, Msg<LineItem>, Msg<RealTimeResult>>.Context context, Collector<Msg<RealTimeResult>> collector) throws Exception {
        Orders orders = ordersMsg.getData();
        if (null == orders) {
            return;
        }
        if (null == alive.value()) {
            alive.update(new HashSet<>());
        }


        if (Status.INSERT == ordersMsg.getStatus()) {
            holder.update(holder.value() == null ? 1 : holder.value()+1);
            attribute.update(ordersMsg.getData());
            if (holder.value() == 1) { // 避免多次，理论没有
//                log.error(JSON.toJSONString(alive.value()));
                Set<LineItem> current = alive.value();
                if (!CollectionUtils.isEmpty(current)) {
                    current.forEach(i -> {
                        collector.collect(collect(ordersMsg.getData(), i, Status.INSERT, true));
                    });
                }
            }
        } else if(Status.DELETE == ordersMsg.getStatus()) {
            holder.update(holder.value() == null ? 0 : holder.value()-1);
            attribute.clear();

            if (holder.value() == 0) {
                Set<LineItem> current = alive.value();
                if (!CollectionUtils.isEmpty(current)) {
                    current.forEach(i -> {
                        collector.collect(collect(ordersMsg.getData(), i, Status.DELETE, true));
                    });
                }
            }
        }
    }

    @Override
    public void processElement2(Msg<LineItem> lineItemMsg, CoProcessFunction<Msg<Orders>, Msg<LineItem>, Msg<RealTimeResult>>.Context context, Collector<Msg<RealTimeResult>> collector) throws Exception, IOException {
        LineItem lineItem = lineItemMsg.getData();
        if (null == lineItem) {
            return;
        }

        if (null == alive.value()) {
            alive.update(new HashSet<>());
        }

        Set<LineItem> current = alive.value();

        if (Status.INSERT == lineItemMsg.getStatus()) {
            current.add(lineItem);
//            ordersMapState.put(orders.getO_custkey(), orders);
            if (null != holder.value() && holder.value() == 1) {
                collector.collect(collect(attribute.value(), lineItem, Status.INSERT, false));
            }
        } else if (Status.DELETE == lineItemMsg.getStatus()) {
            if (holder.value() == 1 && current.contains(lineItem)) { //减少当前这个且得是live的
                collector.collect(collect(attribute.value(), lineItem, Status.DELETE, false));
            }
            current.remove(lineItem);
        }

    }

    /**
     * 看是否是lineitem导致的减少，如果是lineitem的减少，减少就好，
     * 否则就是valid 设置成false进到聚合环节，删除整个的数据
     * @param orders
     * @param lineItem
     * @param status
     * @param deleteall
     * @return
     */
    private Msg<RealTimeResult> collect(Orders orders, LineItem lineItem, Status status, boolean deleteall) {
        if (Status.INSERT == status) {
            RealTimeResult result = new RealTimeResult();
            result.setL_orderkey(lineItem.getL_orderkey());
            result.setO_orderdate(orders.getO_orderdate());
            result.setO_shippriority(orders.getO_shippriority());
            result.setRevenue(lineItem.getL_extendedprice() * (1 - lineItem.getL_discount()));
            result.setValid(true);

            return Msg.<RealTimeResult>builder().status(Status.INSERT).data(result).build();
        } else {
            if (deleteall) {
                RealTimeResult result = new RealTimeResult();
                result.setL_orderkey(lineItem.getL_orderkey());
                result.setO_orderdate(orders.getO_orderdate());
                result.setO_shippriority(orders.getO_shippriority());
                result.setRevenue(0.0);
                result.setValid(false); //表示下游数据会进行删除
                return Msg.<RealTimeResult>builder().status(Status.DELETE).data(result).build();
            } else {
                RealTimeResult result = new RealTimeResult();
                result.setL_orderkey(lineItem.getL_orderkey());
                result.setO_orderdate(orders.getO_orderdate());
                result.setO_shippriority(orders.getO_shippriority());
                result.setRevenue(lineItem.getL_extendedprice() * (1 - lineItem.getL_discount()));
                result.setValid(true);
                return Msg.<RealTimeResult>builder().status(Status.DELETE).data(result).build();
            }
        }
    }


}
