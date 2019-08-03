package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.GlobalConfig;
import com.wanshifu.transformers.common.ConvertException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class CovertUtils {

    private static final Map<Integer, String[]> SUPPORT_DATE_PATTERN = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(CovertUtils.class);

    static {
        SUPPORT_DATE_PATTERN.put(28,
                new String[]{"EEE MMM dd HH:mm:ss zzz yyyy"});
        SUPPORT_DATE_PATTERN.put(23,
                new String[]{"yyyy-MM-dd HH:mm:ss.SSS"});
        SUPPORT_DATE_PATTERN.put(19,
                new String[]{"yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"});
        SUPPORT_DATE_PATTERN.put(17,
                new String[]{"yyyyMMdd HH:mm:ss"});
        SUPPORT_DATE_PATTERN.put(16,
                new String[]{"yyyy-MM-dd HH:mm", "yyyy/MM/dd HH:mm"});
        SUPPORT_DATE_PATTERN.put(14,
                new String[]{"yyyyMMdd HH:mm"});
        SUPPORT_DATE_PATTERN.put(13,
                new String[]{"yyyy-MM-dd HH", "yyyy/MM/dd HH"});
        SUPPORT_DATE_PATTERN.put(11,
                new String[]{"yyyyMMdd HH"});
        SUPPORT_DATE_PATTERN.put(10,
                new String[]{"yyyy-MM-dd", "yyyy/MM/dd"});
        SUPPORT_DATE_PATTERN.put(8,
                new String[]{"yyyyMMdd", "HH:mm:ss", "HH/mm/ss"});
        SUPPORT_DATE_PATTERN.put(6,
                new String[]{"HHmmss"});
        Object extraDateFormat = null;
        try {
            extraDateFormat = GlobalConfig.getGlobalConfig().get(CoreConstants.EXTRA_DATE_FORMAT);
            if (extraDateFormat != null) {
                if (extraDateFormat instanceof Iterable) {
                    ((Iterable) extraDateFormat).forEach(o -> {
                        String s = o.toString();
                        extendPattern(SUPPORT_DATE_PATTERN, s);
                    });
                } else {
                    extendPattern(SUPPORT_DATE_PATTERN, extraDateFormat.toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("extra.date.format 配置错误，已忽略！  {}", extraDateFormat);
        }
    }


    public static Date parse2Date(String str) {
        String[] patterns = SUPPORT_DATE_PATTERN.get(str.length());
        if (patterns == null) {
            long parseLong = tryParseLong(str);
            if (parseLong > 0) {
                return new Date(parseLong);
            }
            throw new ConvertException("unsupported pattern!");
        }
        try {
            return DateUtils.parseDate(str, Locale.CHINA, patterns);
        } catch (ParseException e) {
            throw new ConvertException("unsupported pattern!");
        }
    }


    public static Date parse2Date(String str, String pattern) {
        try {
            return DateUtils.parseDate(str, Locale.CHINA, pattern);
        } catch (ParseException e) {
            throw new ConvertException("unsupported pattern!");
        }
    }

    private static long tryParseLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static void extendPattern(Map<Integer, String[]> supportDatePattern, String s) {
        int len = s.length();
        String[] exists = supportDatePattern.remove(len);
        if (exists == null) {
            supportDatePattern.put(len, new String[]{s});
        } else {
            String[] extend = new String[exists.length + 1];
            System.arraycopy(exists, 0, extend, 1, extend.length - 1);
            extend[0] = s;
            supportDatePattern.put(len, extend);
        }
    }
}
