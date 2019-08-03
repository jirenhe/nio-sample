package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.ConvertException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class BigDecimalColumn extends Column<BigDecimal> {

    public BigDecimalColumn(String name, final String data, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.DOUBLE, sourceType, isPrimaryKey);
        this.setValue(this.parse(data));
    }

    public BigDecimalColumn(String name, Long data, String sourceType, boolean isPrimaryKey) {
        this(name, data == null ? null : String.valueOf(data), sourceType, isPrimaryKey);
    }

    public BigDecimalColumn(String name, Integer data, String sourceType, boolean isPrimaryKey) {
        this(name, data == null ? null : String.valueOf(data), sourceType, isPrimaryKey);
    }

    /**
     * Double无法表示准确的小数数据，我们不推荐使用该方法保存Double数据，建议使用String作为构造入参
     */
    public BigDecimalColumn(String name, final Double data, String sourceType, boolean isPrimaryKey) {
        this(name, data == null ? null
                : new BigDecimal(String.valueOf(data)).toPlainString(), sourceType, isPrimaryKey);
    }

    /**
     * Float无法表示准确的小数数据，我们不推荐使用该方法保存Float数据，建议使用String作为构造入参
     */
    public BigDecimalColumn(String name, final Float data, String sourceType, boolean isPrimaryKey) {
        this(name, data == null ? null
                : new BigDecimal(String.valueOf(data)).toPlainString(), sourceType, isPrimaryKey);
    }

    public BigDecimalColumn(String name, final BigDecimal data, String sourceType, boolean isPrimaryKey) {
        this(name, null == data ? null : data.toPlainString(), sourceType, isPrimaryKey);
    }

    public BigDecimalColumn(String name, final BigInteger data, String sourceType, boolean isPrimaryKey) {
        this(name, null == data ? null : data.toString(), sourceType, isPrimaryKey);
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getValue()) {
            return null;
        }
        return this.getValue();
    }

    @Override
    public Double asDouble() {
        if (null == this.getValue()) {
            return null;
        }
        BigDecimal result = this.getValue();
        OverFlowUtil.validateDoubleNotOverFlow(result);
        return result.doubleValue();
    }

    @Override
    public Long asLong() {
        if (null == this.getValue()) {
            return null;
        }

        BigDecimal result = this.getValue();
        OverFlowUtil.validateLongNotOverFlow(result.toBigInteger());

        return result.longValue();
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getValue()) {
            return null;
        }

        return this.getValue().toBigInteger();
    }

    @Override
    public String asString() {
        if (null == this.getValue()) {
            return null;
        }
        return this.getValue().toString();
    }

    @Override
    public Boolean asBool() {
        throw new ConvertException("Double类型无法转为Bool .");
    }

    @Override
    public Date asDate() {
        throw new ConvertException("Double类型无法转为Date类型 .");
    }

    @Override
    public byte[] asBytes() {
        throw new ConvertException("Double类型无法转为Bytes类型 .");
    }

    private BigDecimal parse(String data) {
        if (null == data) {
            return null;
        }
        if (data.equalsIgnoreCase("NaN") || data.equalsIgnoreCase("-Infinity")
                || data.equalsIgnoreCase("Infinity")) {
            return null;
        }

        try {
            return new BigDecimal(data);
        } catch (Exception e) {
            throw new ConvertException(String.format("String[%s]无法转为Double类型 .", data));
        }
    }


}