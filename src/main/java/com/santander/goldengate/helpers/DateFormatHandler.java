package com.santander.goldengate.helpers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateFormatHandler {

    private static final DateTimeFormatter ISO_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter SPACE_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Normalize epoch millis to ISO timestamp with 'T' and 18-digit fractional seconds
    public String TimeStampNormalizeFromMillis(long millis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        return formatWithFraction(ldt, ISO_SECONDS, 18);
    }

    // Normalize DATE strings to yyyy-MM-dd
    public String NormalizeDateString(String input) {
        if (input == null) return null;
        String norm = input.replace('/', '-');
        int spaceIdx = norm.indexOf(' ');
        int tIdx = norm.indexOf('T');
        int cutIdx = (spaceIdx > 0) ? spaceIdx : (tIdx > 0 ? tIdx : -1);
        String dateOnly = cutIdx > 0 ? norm.substring(0, cutIdx) : norm;
        if (hasIsoDatePrefix(dateOnly)) {
            return dateOnly.substring(0, 10);
        }
        if (isEightDigitDate(dateOnly)) {
            return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
        }
        return dateOnly.length() >= 10 ? dateOnly.substring(0, 10) : dateOnly;
    }

    // Normalize DB2 TIME strings emitted by GoldenGate (HH.mm.ss) to HH:mm:ss.
    public String normalizeTimeString(String input) {
        if (input == null) {
            return null;
        }
        String normalized = input.trim();
        if (normalized.length() == 8
                && isTimeSeparator(normalized.charAt(2))
                && isTimeSeparator(normalized.charAt(5))) {
            return normalized.substring(0, 2) + ':'
                    + normalized.substring(3, 5) + ':'
                    + normalized.substring(6, 8);
        }
        return normalized;
    }

    public String TimeStampNormalize() {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
        return formatWithFraction(ldt, ISO_SECONDS, 18);
    }

    public String formatMillisSpace12(long millis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        return formatWithFraction(ldt, SPACE_SECONDS, 12);
    }

    public String formatTimestampSpace12(String input) {
        if (input == null) {
            return null;
        }
        String normalized = input.trim().replace('T', ' ').replace('/', '-');
        if (normalized.isEmpty()) {
            return normalized;
        }

        int dot = normalized.indexOf('.');
        String base = dot >= 0 ? normalized.substring(0, dot) : normalized;
        if (base.length() > 19) {
            base = base.substring(0, 19);
        }
        StringBuilder fraction = new StringBuilder(12);
        if (dot >= 0) {
            for (int index = dot + 1; index < normalized.length() && fraction.length() < 12; index++) {
                char candidate = normalized.charAt(index);
                if (!Character.isDigit(candidate)) {
                    break;
                }
                fraction.append(candidate);
            }
        }
        while (fraction.length() < 12) {
            fraction.append('0');
        }
        return base + '.' + fraction;
    }

    private String formatWithFraction(
            LocalDateTime value, DateTimeFormatter formatter, int fractionLength) {
        StringBuilder out = new StringBuilder(20 + fractionLength);
        out.append(value.format(formatter)).append('.');
        appendNineDigitNanos(out, value.getNano());
        while (out.length() < 20 + fractionLength) {
            out.append('0');
        }
        return out.toString();
    }

    private void appendNineDigitNanos(StringBuilder out, int nanos) {
        int divisor = 100_000_000;
        while (divisor > 0) {
            out.append((char) ('0' + (nanos / divisor) % 10));
            divisor /= 10;
        }
    }

    private boolean hasIsoDatePrefix(String value) {
        if (value == null || value.length() < 10
                || value.charAt(4) != '-' || value.charAt(7) != '-') {
            return false;
        }
        for (int index = 0; index < 10; index++) {
            if (index != 4 && index != 7 && !Character.isDigit(value.charAt(index))) {
                return false;
            }
        }
        return true;
    }

    private boolean isEightDigitDate(String value) {
        if (value == null || value.length() != 8) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            if (!Character.isDigit(value.charAt(index))) {
                return false;
            }
        }
        return true;
    }

    private boolean isTimeSeparator(char value) {
        return value == '.' || value == ':';
    }
}
