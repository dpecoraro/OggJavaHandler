package com.santander.goldengate.helpers;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import oracle.goldengate.datasource.meta.ColumnMetaData;

public class CharFormatHandler {

    private Set<String> loggedLenCols;

    public CharFormatHandler(Set<String> loggedLenCols) {
        this.loggedLenCols = loggedLenCols;
    }

    public CharFormatHandler() {
        this(ConcurrentHashMap.newKeySet());
    }

    public int safeGetCharLength(ColumnMetaData col) {
        if (col == null) {
            return 255;
        }

        String colName = null;
        try {
            colName = col.getColumnName();
        } catch (Exception ignore) {
        }
        String key = (colName == null ? "UNKNOWN" : colName);

        // loga 1x por coluna
        if (loggedLenCols.add(key)) {
            System.out.println(">>> [LEN-DEBUG] column=" + key
                    + " class=" + col.getClass().getName()
                    + " dataType=" + (col.getDataType() != null ? col.getDataType().toString() : "null"));

            String[] probes = new String[]{
                "getLength", "getCharLength", "getColumnLength", "getDataLength",
                "getBinaryLength", "getDisplaySize", "getPrecision", "getScale"
            };

            for (String mName : probes) {
                try {
                    Method m = col.getClass().getMethod(mName);
                    Object v = m.invoke(col);
                    System.out.println(">>> [LEN-DEBUG] " + key + "." + mName + "=" + v);
                } catch (Exception e) {
                    System.out.println(">>> [LEN-DEBUG] " + key + "." + mName + " (n/a)");
                }
            }
        }

        // tentativa “boa” de char-length
        String[] candidates = new String[]{"getCharLength", "getColumnLength", "getLength"};
        for (String mName : candidates) {
            try {
                Method m = col.getClass().getMethod(mName);
                Object v = m.invoke(col);
                if (v instanceof Number) {
                    int len = ((Number) v).intValue();
                    if (len > 0) {
                        return len;
                    }
                }
            } catch (Exception ignore) {
            }
        }

        // fallback: parse do dataType (se vier algo tipo VARCHAR2(4 CHAR))
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('('), r = dt.indexOf(')');
                if (l > 0 && r > l) {
                    String inside = dt.substring(l + 1, r).trim();
                    if (inside.contains(",")) {
                        inside = inside.substring(0, inside.indexOf(','));
                    }
                    int parsed = Integer.parseInt(inside.replaceAll("[^0-9]", ""));
                    if (parsed > 0) {
                        return parsed;
                    }
                }
            }
        } catch (Exception ignore) {
        }

        return 255;
    }
}
