package com.santander.goldengate.helpers;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class SchemaTypeConverter {
    public Object getDefaultValue(Schema schema) {
        Type type = schema.getType();
        switch (type) {
            case INT:
            case LONG: return 0;
            case FLOAT:
            case DOUBLE: return 0.0;
            case BOOLEAN: return false;
            case STRING: return "";
            case BYTES: return java.nio.ByteBuffer.wrap(new byte[0]);
            default: return null;
        }
    }
}
