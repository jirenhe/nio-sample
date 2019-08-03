package com.wanshifu.transformers.common.bean;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class ObjectColumn extends Column<Object> {

    public ObjectColumn(String name, Object value, String sourceType, boolean isPrimaryKey) {
        super(name, value, Type.UNKNOWN, sourceType, isPrimaryKey);
    }

    @Override
    public Long asLong() {
        return (Long) value;
    }

    @Override
    public Double asDouble() {
        return (Double) value;
    }

    @Override
    public String asString() {
        return value.toString();
    }

    @Override
    public Date asDate() {
        return (Date) value;
    }

    @Override
    public byte[] asBytes() {
        return (byte[]) value;
    }

    @Override
    public Boolean asBool() {
        return (Boolean) value;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return new BigDecimal(value.toString());
    }

    @Override
    public BigInteger asBigInteger() {
        return new BigInteger(value.toString());
    }
}
