package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.ConvertException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class StringColumn extends Column<String> {

    public StringColumn(String name, String value, String sourceType, boolean isPrimaryKey) {
        super(name, value, Type.STRING, sourceType, isPrimaryKey);
    }

    @Override
    public String asString() {
        return this.getValue();
    }

    private void validateDoubleSpecific(final String data) {
        if ("NaN".equals(data) || "Infinity".equals(data)
                || "-Infinity".equals(data)) {
            throw new ConvertException(String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型 .", data));
        }
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getValue()) {
            return null;
        }
        this.validateDoubleSpecific(this.getValue());
        try {
            return this.asBigDecimal().toBigInteger();
        } catch (Exception e) {
            throw new ConvertException(String.format(
                    "String[\"%s\"]不能转为BigInteger .", this.asString()));
        }
    }

    @Override
    public Long asLong() {
        if (null == this.getValue()) {
            return null;
        }

        this.validateDoubleSpecific(this.getValue());

        try {
            BigInteger integer = this.asBigInteger();
            OverFlowUtil.validateLongNotOverFlow(integer);
            return integer.longValue();
        } catch (Exception e) {
            throw new ConvertException(
                    String.format("String[\"%s\"]不能转为Long .", this.asString()));
        }
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getValue()) {
            return null;
        }

        this.validateDoubleSpecific(this.getValue());

        try {
            return new BigDecimal(this.asString());
        } catch (Exception e) {
            throw new ConvertException(String.format("String [\"%s\"] 不能转为BigDecimal .", this.asString()));
        }
    }

    @Override
    public Double asDouble() {
        if (null == this.getValue()) {
            return null;
        }

        String data = this.getValue();
        if ("NaN".equals(data)) {
            return Double.NaN;
        }

        if ("Infinity".equals(data)) {
            return Double.POSITIVE_INFINITY;
        }

        if ("-Infinity".equals(data)) {
            return Double.NEGATIVE_INFINITY;
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

        if ("true".equalsIgnoreCase(this.asString())) {
            return true;
        }

        if ("false".equalsIgnoreCase(this.asString())) {
            return false;
        }

        throw new ConvertException(
                String.format("String[\"%s\"]不能转为Bool .", this.asString()));
    }

    @Override
    public Date asDate() {
        try {
            return CovertUtils.parse2Date(this.getValue());
        } catch (Exception e) {
            throw new ConvertException(
                    String.format("String[\"%s\"]不能转为Date .", this.asString()));
        }
    }

    @Override
    public byte[] asBytes() {
        try {
            return this.getValue().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new ConvertException(
                    String.format("String[\"%s\"]不能转为Bytes .", this.asString()));
        }
    }
}
