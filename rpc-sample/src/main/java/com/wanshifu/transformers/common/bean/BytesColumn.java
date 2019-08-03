package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.ConvertException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class BytesColumn extends Column<byte[]> {

    public BytesColumn(String name, byte[] value, String sourceType, boolean isPrimaryKey) {
        super(name, value, Type.BYTES, sourceType, isPrimaryKey);
    }

    @Override
    public byte[] asBytes() {
        if (null == this.getValue()) {
            return null;
        }

        return this.getValue();
    }

    @Override
    public String asString() {
        if (null == this.getValue()) {
            return null;
        }

        try {
            return new String(this.value);
        } catch (Exception e) {
            throw new ConvertException(String.format("Bytes[%s]不能转为String .", this.toString()));
        }
    }

    @Override
    public Long asLong() {
        throw new ConvertException("Bytes类型不能转为Long .");
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new ConvertException("Bytes类型不能转为BigDecimal .");
    }

    @Override
    public BigInteger asBigInteger() {
        throw new ConvertException("Bytes类型不能转为BigInteger .");
    }

    @Override
    public Double asDouble() {
        throw new ConvertException("Bytes类型不能转为Long .");
    }

    @Override
    public Date asDate() {
        throw new ConvertException("Bytes类型不能转为Date .");
    }

    @Override
    public Boolean asBool() {
        throw new ConvertException("Bytes类型不能转为Boolean .");
    }
}
