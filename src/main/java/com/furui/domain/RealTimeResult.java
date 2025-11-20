package com.furui.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RealTimeResult implements Serializable {
    /**
     * 分组键：订单ID
     */
    private long l_orderkey;

    /**
     *  订单日期（关联订单属性）
     */
    private String o_orderdate;

    /**
     * 发货优先级
     */
    private int o_shippriority;

    /**
     * 累计收入
     */
    private double revenue;
}
