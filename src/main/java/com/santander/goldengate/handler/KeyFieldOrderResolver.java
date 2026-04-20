package com.santander.goldengate.handler;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToIntFunction;

final class KeyFieldOrderResolver {

    private KeyFieldOrderResolver() {
    }

    static <T> List<T> order(List<T> fields, ToIntFunction<T> keyIndexExtractor, ToIntFunction<T> tableIndexExtractor) {
        List<T> ordered = new ArrayList<>(fields);
        ordered.sort(Comparator
                .comparingInt((T field) -> {
                    int keyIndex = keyIndexExtractor.applyAsInt(field);
                    return keyIndex >= 0 ? keyIndex : Integer.MAX_VALUE;
                })
                .thenComparingInt(tableIndexExtractor));
        return ordered;
    }
}
