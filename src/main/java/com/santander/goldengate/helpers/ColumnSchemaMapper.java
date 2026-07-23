package com.santander.goldengate.helpers;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import oracle.goldengate.datasource.meta.ColumnDataType;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DataTypes;

/**
 * Single source of truth for the DB2-compatible Avro column contract.
 */
public final class ColumnSchemaMapper {

    public static final class Mapping {
        private final Schema schema;
        private final Object defaultValue;

        private Mapping(Schema schema, Object defaultValue) {
            this.schema = schema;
            this.defaultValue = defaultValue;
        }

        public Schema getSchema() {
            return schema;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }
    }

    public Mapping map(ColumnMetaData column) {
        if (column == null) {
            throw new IllegalArgumentException("Column metadata is required");
        }

        ColumnDataType type = column.getColumnDataType();
        DataTypes sourceType = column.getSourceColumnDataType();
        DataTypes dataType = type != null ? type.getDataTypeEnum() : null;
        String nativeType = type != null && type.getNativeDataType() != null
                ? type.getNativeDataType().trim().toUpperCase()
                : "";
        String descriptiveType = column.getDataType() != null
                ? column.getDataType().toString().toUpperCase()
                : "";
        String typeName = nativeType + " " + descriptiveType;

        if (isSmallInt(nativeType, typeName)) {
            Schema schema = primitive(column, Type.INT, "SMALLINT");
            return new Mapping(schema, 0);
        }

        if (isFixedPoint(sourceType, dataType, typeName)) {
            int precision = resolvePrecision(column, type);
            int scale = resolveScale(type);
            Type avroType = scale == 0
                    ? precision <= 9 ? Type.INT : precision <= 18 ? Type.LONG : Type.STRING
                    : Type.STRING;
            Schema schema = primitive(column, avroType, "DECIMAL");
            schema.addProp("precision", precision);
            schema.addProp("scale", scale);
            return new Mapping(schema, avroType == Type.STRING ? "0" : 0);
        }

        if (isInteger(sourceType, dataType, typeName)) {
            Type avroType = typeName.contains("BIGINT") ? Type.LONG : Type.INT;
            Schema schema = primitive(column, avroType, "INTEGER");
            return new Mapping(schema, 0);
        }

        if (isFloatingPoint(sourceType, dataType, typeName)) {
            return new Mapping(primitive(column, Type.DOUBLE, "DOUBLE"), 0.0d);
        }

        if (isDate(column, nativeType, typeName)) {
            Schema schema = primitive(column, Type.STRING, "DATE");
            schema.addProp("length", 10);
            return new Mapping(schema, "");
        }

        if (isTime(nativeType)) {
            Schema schema = primitive(column, Type.STRING, "TIME");
            schema.addProp("length", 8);
            return new Mapping(schema, "");
        }

        if (isTimestamp(column, sourceType, dataType, typeName)) {
            Schema schema = primitive(column, Type.STRING, "TIMESTAMP");
            schema.addProp("length", 32);
            return new Mapping(schema, "");
        }

        if ("VARCHAR".equals(nativeType)) {
            int length = resolveCharacterLength(column, type);
            Schema schema = primitive(column, Type.STRING, "VARCHAR");
            schema.addProp("length", length);
            return new Mapping(schema, "");
        }

        if (isCharacter(sourceType, dataType, typeName)) {
            int length = resolveCharacterLength(column, type);
            Schema schema = primitive(column, Type.STRING, "CHARACTER");
            schema.addProp("length", length);
            return new Mapping(schema, "");
        }

        if (isBinary(sourceType, dataType, typeName)) {
            return new Mapping(primitive(column, Type.BYTES, "BINARY"),
                    java.nio.ByteBuffer.wrap(new byte[0]));
        }

        throw new IllegalArgumentException("Unsupported column type for " + column.getColumnName()
                + ": sourceType=" + sourceType + ", dataType=" + dataType + ", nativeType=" + nativeType);
    }

    private Schema primitive(ColumnMetaData column, Type type, String logicalType) {
        Schema schema = Schema.create(type);
        schema.addProp("logicalType", logicalType);
        schema.addProp("dbColumnName", column.getColumnName());
        return schema;
    }

    private int resolvePrecision(ColumnMetaData column, ColumnDataType type) {
        long precision = type != null ? type.getPrecision() : -1;
        if (precision <= 0 && column.getMostSignificantDigit() > 0) {
            precision = column.getMostSignificantDigit();
        }
        if (precision <= 0 || precision > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Missing decimal precision for column " + column.getColumnName());
        }
        return (int) precision;
    }

    private int resolveScale(ColumnDataType type) {
        int scale = type != null ? type.getScale() : -1;
        if (scale < 0) {
            throw new IllegalArgumentException("Missing decimal scale");
        }
        return scale;
    }

    private int resolveCharacterLength(ColumnMetaData column, ColumnDataType type) {
        long length = type != null ? type.getColumnLength() : -1;
        if (length <= 0) {
            length = column.getColumnLength();
        }
        if (length <= 0 || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Missing character length for column " + column.getColumnName());
        }
        return (int) length;
    }

    private boolean isSmallInt(String nativeType, String typeName) {
        return "SMALLINT".equals(nativeType) || typeName.contains("SMALLINT");
    }

    private boolean isFixedPoint(DataTypes sourceType, DataTypes dataType, String typeName) {
        return sourceType == DataTypes.T_FixedPoint
                || dataType == DataTypes.T_FixedPoint
                || typeName.contains("DECIMAL")
                || typeName.contains("NUMERIC")
                || typeName.contains("NUMBER");
    }

    private boolean isInteger(DataTypes sourceType, DataTypes dataType, String typeName) {
        return sourceType == DataTypes.T_Integer
                || dataType == DataTypes.T_Integer
                || typeName.contains("INT");
    }

    private boolean isFloatingPoint(DataTypes sourceType, DataTypes dataType, String typeName) {
        return sourceType == DataTypes.T_FloatingPoint
                || dataType == DataTypes.T_FloatingPoint
                || typeName.contains("FLOAT")
                || typeName.contains("DOUBLE")
                || typeName.contains("REAL");
    }

    private boolean isDate(ColumnMetaData column, String nativeType, String typeName) {
        return (column.getColumnName() != null
                && column.getColumnName().toUpperCase().startsWith("DT_"))
                || "DATE".equals(nativeType)
                || typeName.contains(" DATE")
                && !typeName.contains("TIMESTAMP")
                && !typeName.contains("DATETIME");
    }

    private boolean isTimestamp(
            ColumnMetaData column,
            DataTypes sourceType,
            DataTypes dataType,
            String typeName) {
        return sourceType == DataTypes.T_DateTime
                || dataType == DataTypes.T_DateTime
                || typeName.contains("TIMESTAMP")
                || typeName.contains("DATETIME")
                || column.isTimestamp()
                || column.isTime();
    }

    private boolean isTime(String nativeType) {
        return "TIME".equals(nativeType);
    }

    private boolean isCharacter(DataTypes sourceType, DataTypes dataType, String typeName) {
        return sourceType == DataTypes.T_Character
                || dataType == DataTypes.T_Character
                || typeName.contains("CHAR")
                || typeName.contains("TEXT");
    }

    private boolean isBinary(DataTypes sourceType, DataTypes dataType, String typeName) {
        return sourceType == DataTypes.T_Binary
                || dataType == DataTypes.T_Binary
                || typeName.contains("BLOB")
                || typeName.contains("BINARY");
    }
}
