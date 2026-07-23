package com.santander.goldengate.helpers;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Converts decimal values according to the schema representation selected by
 * {@link ColumnSchemaMapper}.
 */
public final class DecimalValueConverter {

    public Object convert(Object value, Schema schema, String fieldName) {
        Object scaleProp = schema.getObjectProp("scale");
        if (!(scaleProp instanceof Number)) {
            throw new IllegalArgumentException("Decimal scale is missing for " + fieldName);
        }

        int scale = ((Number) scaleProp).intValue();
        String normalized = value.toString().trim().replace(',', '.');
        BigDecimal decimal = new BigDecimal(normalized)
                .setScale(scale, RoundingMode.UNNECESSARY);
        Type type = schema.getType();

        switch (type) {
            case STRING:
                return decimal.toPlainString();
            case LONG:
                return decimal.longValueExact();
            case INT:
                return decimal.intValueExact();
            case DOUBLE:
                return decimal.doubleValue();
            case FLOAT:
                return decimal.floatValue();
            default:
                throw new IllegalArgumentException("Unsupported Avro decimal type: " + type);
        }
    }
}
