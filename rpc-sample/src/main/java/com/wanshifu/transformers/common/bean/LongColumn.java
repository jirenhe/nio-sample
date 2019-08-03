package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.ConvertException;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class LongColumn extends Column<BigInteger> {

    public LongColumn(String name, Long value, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.LONG, sourceType, isPrimaryKey);
        this.setValue(BigInteger.valueOf(value));
    }

    public LongColumn(String name, Integer value, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.LONG, sourceType, isPrimaryKey);
        this.setValue(BigInteger.valueOf(value));
    }

    public LongColumn(String name, BigInteger value, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.LONG, sourceType, isPrimaryKey);
        this.setValue(value);
    }

    /**
     * 从整形字符串表示转为LongColumn，支持Java科学计数法
     * <p>
     * NOTE: <br>
     * 如果data为浮点类型的字符串表示，数据将会失真，请使用DoubleColumn对接浮点字符串
     */
    public LongColumn(String name, final String data, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.LONG, sourceType, isPrimaryKey);
        if (null != data) {
            try {
                BigInteger rawData = NumberUtils.createBigDecimal(data).toBigInteger();
                this.setValue(rawData);
            } catch (Exception e) {
                throw new ConvertException(String.format("String[%s]不能转为Long .", data));
            }
        }
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getValue()) {
            return null;
        }

        return this.getValue();
    }

    @Override
    public Long asLong() {
        BigInteger rawData = this.getValue();
        if (null == rawData) {
            return null;
        }

        OverFlowUtil.validateLongNotOverFlow(rawData);

        return rawData.longValue();
    }

    @Override
    public Double asDouble() {
        if (null == this.getValue()) {
            return null;
        }

        BigDecimal decimal = this.asBigDecimal();
        OverFlowUtil.validateDoubleNotOverFlow(decimal);

        return decimal.doubleValue();
    }

    @Override
    public Boolean asBool() {
        if (null == this.getValue()) {
            return null;
        }

        return this.asBigInteger().compareTo(BigInteger.ZERO) != 0;
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getValue()) {
            return null;
        }

        return new BigDecimal(this.asBigInteger());
    }

    @Override
    public String asString() {
        if (null == this.getValue()) {
            return null;
        }
        return this.getValue().toString();
    }

    @Override
    public Date asDate() {
        if (null == this.getValue()) {
            return null;
        }
        return new Date(this.asLong());
    }

    @Override
    public byte[] asBytes() {
        throw new ConvertException("Long类型不能转为Bytes .");
    }

}
