package com.furui.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LineItem implements Serializable {
    private int l_orderkey;
    private int l_linenumber;
    private int l_partkey;
    private int l_suppkey;
    private int l_quantity;
    private double l_extendedprice;
    private double l_discount;
    private double l_tax;
    private char l_returnflag;
    private char l_linestatus;
    private String l_shipdate;
    private Date l_commitdate;
    private Date l_receiptdate;
    private String l_shipinstruct;
    private String l_shipmode;
    private String l_comment;
}
