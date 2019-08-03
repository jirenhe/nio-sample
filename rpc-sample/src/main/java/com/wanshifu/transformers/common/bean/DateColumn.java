package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.ConvertException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateColumn extends Column<Date> {

    private DateType subType;

    public DateColumn(String name, Date value, String sourceType, boolean isPrimaryKey) {
        super(name, value, Type.DATE, sourceType, isPrimaryKey);
        this.subType = DateType.DATETIME;
    }

    public DateColumn(String name, final Long stamp, String sourceType, boolean isPrimaryKey) {
        super(name, stamp == null ? null : new Date(stamp), Type.DATE, sourceType, isPrimaryKey);
        this.subType = DateType.DATETIME;
    }

    public DateColumn(String name, final Long stamp, String sourceType, boolean isPrimaryKey, DateType subType) {
        super(name, stamp == null ? null : new Date(stamp), Type.DATE, sourceType, isPrimaryKey);
        this.subType = subType;
    }

    /**
     * 构建值为date(java.sql.Date)的DateColumn，使用Date子类型为DATE，只有日期，没有时间
     */
    public DateColumn(String name, final java.sql.Date date, String sourceType, boolean isPrimaryKey) {
        this(name, date == null ? null : date.getTime(), sourceType, isPrimaryKey, DateType.DATE);
    }

    /**
     * 构建值为time(java.sql.Time)的DateColumn，使用Date子类型为TIME，只有时间，没有日期
     */
    public DateColumn(String name, final java.sql.Time time, String sourceType, boolean isPrimaryKey) {
        this(name, time == null ? null : time.getTime(), sourceType, isPrimaryKey, DateType.TIME);
    }

    /**
     * 构建值为ts(java.sql.Timestamp)的DateColumn，使用Date子类型为DATETIME
     */
    public DateColumn(String name, final java.sql.Timestamp ts, String sourceType, boolean isPrimaryKey) {
        this(name, ts == null ? null : ts.getTime(), sourceType, isPrimaryKey, DateType.DATETIME);
    }

    public DateColumn(String name, String date, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.DATE, sourceType, isPrimaryKey);
        this.subType = DateType.DATE;
        this.setValue(date == null ? null : CovertUtils.parse2Date(date));
    }

    public DateColumn(String name, String date, String pattern, String sourceType, boolean isPrimaryKey) {
        super(name, null, Type.DATE, sourceType, isPrimaryKey);
        this.subType = DateType.DATE;
        this.setValue(date == null ? null : CovertUtils.parse2Date(date, pattern));
    }

    @Override
    public Long asLong() {
        return this.getValue().getTime();
    }

    @Override
    public String asString() {
        try {
            return new SimpleDateFormat(this.subType.formatPattern()).format(this.getValue());
        } catch (Exception e) {
            throw new ConvertException(String.format("Date[%s]类型不能转为String .", this.toString()));
        }
    }

    @Override
    public Date asDate() {
        if (null == this.getValue()) {
            return null;
        }

        return new Date(this.getValue().getTime());
    }

    @Override
    public byte[] asBytes() {
        throw new ConvertException("Date类型不能转为Bytes .");
    }

    @Override
    public Boolean asBool() {
        return this.getValue() == null ? null : this.asLong() > 0;
    }

    @Override
    public Double asDouble() {
        return this.getValue() == null ? null : new Double(this.asLong().toString());
    }

    @Override
    public BigInteger asBigInteger() {
        return this.getValue() == null ? null : BigInteger.valueOf(this.asLong());
    }

    @Override
    public BigDecimal asBigDecimal() {
        return this.getValue() == null ? null : BigDecimal.valueOf(this.asLong());
    }

    public DateType getSubType() {
        return subType;
    }

    public void setSubType(DateType subType) {
        this.subType = subType;
    }

    public enum DateType {
        DATE {
            @Override
            public String formatPattern() {
                return "yyyy-MM-dd HH:mm:ss";
            }
        },
        TIME {
            @Override
            public String formatPattern() {
                return "HH:mm:ss";
            }
        },
        DATETIME {
            @Override
            public String formatPattern() {
                return "yyyy-MM-dd";
            }
        };

        public abstract String formatPattern();
    }
}