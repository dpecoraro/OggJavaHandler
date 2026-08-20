package com.santander.goldengate.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.santander.goldengate.helpers.DateFormatHandler;

class OperationTimestampResolverTest {

    private final DateFormatHandler formatter = new DateFormatHandler();

    @Test
    void prefersGoldenGateOperationTimestamp() {
        assertEquals("2026-08-20 10:27:04.123456000000",
                OperationTimestampResolver.resolve(
                        "2026-08-20T10:27:04.123456",
                        "2026-08-20 10:28:05",
                        formatter,
                        () -> 0L));
    }

    @Test
    void fallsBackToGoldenGateTransactionTimestamp() {
        assertEquals("2026-08-20 10:28:05.000000000000",
                OperationTimestampResolver.resolve(
                        null,
                        "2026-08-20 10:28:05",
                        formatter,
                        () -> 0L));
    }

    @Test
    void usesClockOnlyWhenGoldenGateHasNoTimestamp() {
        String expected = formatter.formatMillisSpace12(123_456L);

        assertEquals(expected,
                OperationTimestampResolver.resolve(" ", null, formatter, () -> 123_456L));
    }
}
