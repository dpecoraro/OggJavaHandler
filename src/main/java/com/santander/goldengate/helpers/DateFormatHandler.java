package com.santander.goldengate.helpers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateFormatHandler {

    // Normalize epoch millis to ISO timestamp with 'T' and 18-digit fractional seconds
    public String TimeStampNormalizeFromMillis(long millis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        String base = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        String frac9 = String.format("%09d", ldt.getNano());
        StringBuilder frac18 = new StringBuilder(frac9);
        while (frac18.length() < 18) frac18.append('0');
        return base + "." + frac18;
    }

    // Normalize DATE strings to yyyy-MM-dd
    public String NormalizeDateString(String input) {
        if (input == null) return null;
        String norm = input.replace('/', '-');
        int spaceIdx = norm.indexOf(' ');
        int tIdx = norm.indexOf('T');
        int cutIdx = (spaceIdx > 0) ? spaceIdx : (tIdx > 0 ? tIdx : -1);
        String dateOnly = cutIdx > 0 ? norm.substring(0, cutIdx) : norm;
        if (dateOnly.length() >= 10 && dateOnly.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            return dateOnly.substring(0, 10);
        }
        if (dateOnly.matches("\\d{8}")) {
            return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
        }
        return dateOnly.length() >= 10 ? dateOnly.substring(0, 10) : dateOnly;
    }

    public String TimeStampNormalize() {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
        String base = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        int nanos = ldt.getNano(); 
        String frac9 = String.format("%09d", nanos);
        StringBuilder frac18 = new StringBuilder(frac9);
        while (frac18.length() < 18) frac18.append('0');
        return base + "." + frac18;
    }
}
