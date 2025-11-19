package com.furui.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RealTimeOrderRevenue {
    /**
     * 分组键：订单ID
     */
    private long orderkey;

    /**
     *  订单日期（关联订单属性）
     */
    private String orderdate;

    /**
     * 发货优先级
     */
    private int shippriority;

    /**
     * 累计收入
     */
    private double currentTotal;
}
