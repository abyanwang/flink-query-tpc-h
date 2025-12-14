package com.furui.general;

import com.furui.constant.Status;
import com.furui.domain.Msg;
import com.furui.domain.Orders;
import com.furui.domain.RealTimeResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class LineItemAggregationProcess extends KeyedProcessFunction<Long, Msg<RealTimeResult>, RealTimeResult> {

    private ValueState<Double> totalRevenueState;

    @Override
    public void open(Configuration parameters) {
        totalRevenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("total_revenue", Types.DOUBLE)
        );

    }

    @Override
    public void processElement(Msg<RealTimeResult> realTimeResultMsg, KeyedProcessFunction<Long, Msg<RealTimeResult>, RealTimeResult>.Context context, Collector<RealTimeResult> collector) throws Exception {
        if (realTimeResultMsg.getStatus() == Status.INSERT) {
            RealTimeResult result = realTimeResultMsg.getData();
            if (null != totalRevenueState.value()) {
                totalRevenueState.update(totalRevenueState.value() + result.getRevenue());
                collector.collect(collect(result, totalRevenueState.value() + result.getRevenue()));
            } else {
                totalRevenueState.update(result.getRevenue());
                collector.collect(collect(result, result.getRevenue()));
            }
            return ;
        } else {
            RealTimeResult result = realTimeResultMsg.getData();
            if (!result.isValid()) {
                totalRevenueState.clear();
                collector.collect(delete(result));
            } else {
                if (null != totalRevenueState.value()) {
                    totalRevenueState.update(totalRevenueState.value() - result.getRevenue()); //这里先不考虑扣减为0的事情，如果需要可以添加
                    collector.collect(collect(result, totalRevenueState.value() - result.getRevenue()));
                } else {
                    //这里也是不对的状态，先扣减，先在这里写着可以根绝业务规则修改
                    totalRevenueState.update(-result.getRevenue());
                    collector.collect(collect(result, -result.getRevenue()));
                }
            }
        }
    }

    private RealTimeResult collect(RealTimeResult in, double revenue) {
        RealTimeResult out = new RealTimeResult();
        out.setL_orderkey(in.getL_orderkey());
        out.setO_orderdate(in.getO_orderdate());
        out.setO_shippriority(in.getO_shippriority());
        out.setValid(true);
        out.setRevenue(revenue);
        return out;
    }

    private RealTimeResult delete(RealTimeResult in) {
        RealTimeResult out = new RealTimeResult();
        out.setL_orderkey(in.getL_orderkey());
        out.setO_orderdate(in.getO_orderdate());
        out.setO_shippriority(in.getO_shippriority());
        out.setValid(false);
        out.setRevenue(0.0);
        return out;
    }
}
