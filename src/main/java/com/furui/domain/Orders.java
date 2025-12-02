package com.furui.domain;

import com.furui.constant.Status;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Orders implements Serializable {

    private int o_orderkey;
    private int o_custkey;
    private String o_orderstatus;
    private double o_totalprice;
    private String  o_orderdate;
    private String o_orderpriority;
    private String o_clerk;
    private int o_shippriority;
    private String o_comment;

    /**
     * 默认insert
     */
    private Status status = Status.INSERT;

    public static Orders convert(String line) {
        String[] fields = line.split("\\|");
        Orders orders = new Orders();
        orders.setO_orderkey(Integer.parseInt(fields[0]));
        orders.setO_custkey(Integer.parseInt(fields[1]));
        orders.setO_orderdate(fields[4]);
        orders.setO_shippriority(Integer.parseInt(fields[7]));
        return orders;
    }
}
