package com.wanshifu.transformers.common.bean;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

@Data
public abstract class Column<T> implements Serializable, Cloneable {

    protected String name;

    protected T value;

    protected Type type;

    protected String sourceType;

    protected boolean isPrimaryKey;

    protected Column(String name, T value, Type type, String sourceType, boolean isPrimaryKey) {
        this.name = name;
        this.value = value;
        this.type = type;
        this.sourceType = sourceType;
        this.isPrimaryKey = isPrimaryKey;
    }

    @Override
    public Column clone() {
        try {
            return (Column) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("this should never happened!");
        }
    }

    public abstract Long asLong();

    public abstract Double asDouble();

    public abstract String asString();

    public abstract Date asDate();

    public abstract byte[] asBytes();

    public abstract Boolean asBool();

    public abstract BigDecimal asBigDecimal();

    public abstract BigInteger asBigInteger();

    public enum Type {
        BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES, UNKNOWN
    }
}
