package com.furui.domain;

import com.furui.constant.Status;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer implements Serializable {
    /**
     * 关联键
     */
    private int c_custkey;
    private String c_name;
    private String c_address;
    private int c_nationkey;
    private String c_phone;
    private double c_acctbal;

    /**
     *
     */
    private String c_mktsegment;
    private String c_comment;

    /**
     * 默认insert
     */
    private Status status = Status.INSERT;


    public static Customer convert(String line) {
        String[] fields = line.split("\\|");
        Customer customer = new Customer();
        customer.setC_custkey(Integer.parseInt(fields[0]));
        customer.setC_mktsegment(fields[6]);
        if (fields.length == 9) {
            customer.setStatus(Status.valueOf(fields[8]));
        }
        return customer;
    }
}
