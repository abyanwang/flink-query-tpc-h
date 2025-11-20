package com.furui.domain;

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
}
