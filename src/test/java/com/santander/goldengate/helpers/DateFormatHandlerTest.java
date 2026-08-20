package com.santander.goldengate.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DateFormatHandlerTest {

    private final DateFormatHandler formatter = new DateFormatHandler();

    @Test
    void normalizesGoldenGateTimestampAndPadsFractionToTwelveDigits() {
        assertEquals("2026-08-20 10:27:04.123000000000",
                formatter.formatTimestampSpace12("2026-08-20T10:27:04.123"));
    }

    @Test
    void truncatesFractionsLongerThanTwelveDigits() {
        assertEquals("2026-08-20 10:27:04.123456789012",
                formatter.formatTimestampSpace12("2026-08-20 10:27:04.123456789012345678"));
    }

    @Test
    void addsAZeroFractionWhenTimestampHasNoFraction() {
        assertEquals("2026-08-20 10:27:04.000000000000",
                formatter.formatTimestampSpace12("2026/08/20 10:27:04"));
    }

    @Test
    void formatsEpochMillisWithoutAllocatingPerCallFormatter() {
        String formatted = formatter.formatMillisSpace12(0L);

        assertEquals(32, formatted.length());
        assertTrue(formatted.endsWith(".000000000000"));
    }
}
