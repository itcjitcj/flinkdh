package com.itcj.FlinkSql.util;

import org.apache.flink.table.functions.ScalarFunction;

// define function logic
public class SubstringFunction extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}
