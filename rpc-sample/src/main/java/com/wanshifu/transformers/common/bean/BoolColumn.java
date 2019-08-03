package com.wanshifu.transformers.common.bean;


import com.wanshifu.transformers.common.ConvertException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class BoolColumn extends Column<Boolean> {

    public BoolColumn(String name, Boolean value, String sourceType, boolean isPrimaryKey) {
        super(name, value, Type.BOOL, sourceType, isPrimaryKey);
    }

    public BoolColumn(String name, String data, String sourceType, boolean isPrimaryKey) {
        this(name, true, sourceType, isPrimaryKey);
        this.setValue(this.parse(data));
    }

    @Override
    public Boolean asBool() {
        if (null == this.getValue()) {
            return null;
        }

        return this.getValue();
    }

    @Override
    public Long asLong() {
        if (null == this.getValue()) {
            return null;
        }

        return this.asBool() ? 1L : 0L;
    }

    @Override
    public Double asDouble() {
        if (null == this.getValue()) {
            return null;
        }

        return this.asBool() ? 1.0d : 0.0d;
    }

    @Override
    public String asString() {
        if (null == this.getValue()) {
            return null;
        }

        return this.asBool() ? "true" : "false";
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getValue()) {
            return null;
        }

        return BigInteger.valueOf(this.asLong());
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getValue()) {
            return null;
        }

        return BigDecimal.valueOf(this.asLong());
    }

    @Override
    public Date asDate() {
        throw new ConvertException("Bool类型不能转为Date .");
    }

    @Override
    public byte[] asBytes() {
        throw new ConvertException("Boolean类型不能转为Bytes .");
    }

    private Boolean parse(String data) {
        if (null == data) {
            return null;
        }
        if ("true".equalsIgnoreCase(data)) {
            return Boolean.TRUE;
        } else if ("false".equalsIgnoreCase(data)) {
            return Boolean.FALSE;
        }

        throw new ConvertException(String.format("String[%s]不能转为Bool .", data));
    }
}
