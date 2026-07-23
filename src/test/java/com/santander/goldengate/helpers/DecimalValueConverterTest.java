package com.santander.goldengate.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;

class DecimalValueConverterTest {
    private final DecimalValueConverter converter = new DecimalValueConverter();

    @Test
    void convertsIntegralDecimalWithoutParsingThroughDouble() {
        Schema schema = decimal(Type.INT, 9, 0);
        assertEquals(1, converter.convert("1.00", schema, "VL_INT"));
    }

    @Test
    void preservesDeclaredStringScale() {
        Schema schema = decimal(Type.STRING, 19, 2);
        assertEquals("1.20", converter.convert("1.2", schema, "VL_STRING"));
    }

    @Test
    void rejectsUnexpectedRoundingAndOverflow() {
        assertThrows(ArithmeticException.class,
                () -> converter.convert("1.1", decimal(Type.INT, 9, 0), "VL_INT"));
        assertThrows(ArithmeticException.class,
                () -> converter.convert("2147483648", decimal(Type.INT, 9, 0), "VL_INT"));
    }

    private Schema decimal(Type type, int precision, int scale) {
        Schema schema = Schema.create(type);
        schema.addProp("logicalType", "DECIMAL");
        schema.addProp("precision", precision);
        schema.addProp("scale", scale);
        return schema;
    }
}
