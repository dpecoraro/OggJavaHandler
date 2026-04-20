package com.santander.goldengate.handler;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class KeyFieldOrderResolverTest {

    @Test
    void orderUsesKeyIndexBeforeTableOrder() {
        List<TestField> ordered = KeyFieldOrderResolver.order(
                Arrays.asList(
                        new TestField("CD_CENT_CPTU", 1, 0),
                        new TestField("CD_BANC", 0, 1),
                        new TestField("AN_PROP", 2, 2)),
                field -> field.keyIndex,
                field -> field.tableIndex);

        assertIterableEquals(
                Arrays.asList("CD_BANC", "CD_CENT_CPTU", "AN_PROP"),
                ordered.stream().map(field -> field.name).collect(Collectors.toList()));
    }

    @Test
    void orderFallsBackToTableOrderWhenKeyIndexIsMissing() {
        List<TestField> ordered = KeyFieldOrderResolver.order(
                Arrays.asList(
                        new TestField("FIRST", -1, 0),
                        new TestField("SECOND", -1, 1),
                        new TestField("THIRD", -1, 2)),
                field -> field.keyIndex,
                field -> field.tableIndex);

        assertIterableEquals(
                Arrays.asList("FIRST", "SECOND", "THIRD"),
                ordered.stream().map(field -> field.name).collect(Collectors.toList()));
    }

    private static final class TestField {
        private final String name;
        private final int keyIndex;
        private final int tableIndex;

        private TestField(String name, int keyIndex, int tableIndex) {
            this.name = name;
            this.keyIndex = keyIndex;
            this.tableIndex = tableIndex;
        }
    }
}
