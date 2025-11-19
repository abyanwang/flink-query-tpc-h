package com.furui.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer implements Serializable {
    private int c_custkey;
    private String c_name;
    private String c_address;
    private int c_nationkey;
    private String c_phone;
    private double c_acctbal;
    private String c_mktsegment;
    private String c_comment;
}
