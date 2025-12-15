package com.furui.domain;

import com.furui.constant.Status;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

import static org.apache.commons.math3.exception.util.LocalizedFormats.SCALE;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LineItem implements Serializable {
    /**
     * 关联键
     */
    private int l_orderkey;
    private int l_linenumber;
    private int l_partkey;
    private int l_suppkey;
    private int l_quantity;
    private BigDecimal l_extendedprice;
    private BigDecimal l_discount;
    private double l_tax;
    private char l_returnflag;
    private char l_linestatus;
    private String l_shipdate;
    private Date l_commitdate;
    private Date l_receiptdate;
    private String l_shipinstruct;
    private String l_shipmode;
    private String l_comment;

    public BigDecimal cal() {
        BigDecimal oneMinusDiscountBd = BigDecimal.ONE.subtract(this.l_discount);

        return this.l_extendedprice.multiply(oneMinusDiscountBd)
                .setScale(2, RoundingMode.HALF_UP);
    }

    public static Msg< LineItem> convert(String line) {
        String[] fields = line.split("\\|");
        Msg<LineItem> msg = new Msg<>();
        LineItem lineitem = new LineItem();
        lineitem.setL_orderkey(Integer.parseInt(fields[0]));
        lineitem.setL_extendedprice(new BigDecimal(fields[5]));
        lineitem.setL_discount(new BigDecimal(fields[6]));
        lineitem.setL_shipdate(fields[10]);
        if ("DELETE".equals(fields[fields.length-1])) {
            msg.setStatus(Status.DELETE);
        }
        msg.setData(lineitem);
        return msg;
    }
}
