package com.santander.goldengate.handler;

import java.util.function.LongSupplier;

import com.santander.goldengate.helpers.DateFormatHandler;

final class OperationTimestampResolver {

    private OperationTimestampResolver() {
    }

    static String resolve(
            String operationTimestamp,
            String transactionTimestamp,
            DateFormatHandler formatter,
            LongSupplier fallbackClock) {
        String selected = hasText(operationTimestamp)
                ? operationTimestamp
                : transactionTimestamp;
        if (!hasText(selected)) {
            return formatter.formatMillisSpace12(fallbackClock.getAsLong());
        }
        return formatter.formatTimestampSpace12(selected);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
