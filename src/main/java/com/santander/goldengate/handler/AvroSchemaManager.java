package com.santander.goldengate.handler;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.meta.ColumnDataType;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DataTypes;
import oracle.goldengate.datasource.meta.TableMetaData;

/**
 * Manages Avro schema creation and caching for GoldenGate data replication.
 */
public class AvroSchemaManager {

    private final Map<String, Schema> schemaCache = new HashMap<>();
    private final String namespacePrefix;
    private final SchemaTypeConverter schemaTypeConverter;

    public AvroSchemaManager(String namespacePrefix, SchemaTypeConverter schemaTypeConverter) {
        this.namespacePrefix = namespacePrefix;
        this.schemaTypeConverter = schemaTypeConverter;
    }

    public Schema getOrCreateAvroSchema(String tableName, TableMetaData tableMetaData) {
        Schema cached = schemaCache.get(tableName);
        if (cached != null) return cached;

        final String rawName = tableName != null && tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1)
                : tableName;
        final String tableRecordName = rawName != null ? rawName.toUpperCase() : "UNKNOWN";
        System.out.println(">>> [AvroSchemaManager] Value schema table record name: raw='" + rawName + "' -> upper='" + tableRecordName + "'");

        // Build table record schema
        List<Field> tableFields = new ArrayList<>();

        // Always iterate using safeGetColumnMetaData to cover all columns
        if (tableMetaData != null) {
            for (int idx = 0; ; idx++) {
                ColumnMetaData cm = safeGetColumnMetaData(tableMetaData, idx);
                if (cm == null) break;
                String colName = cm.getColumnName();
                Schema colSchema = buildColumnSchema(cm);
                Object defaultValue = schemaTypeConverter.getDefaultValue(colSchema);
                tableFields.add(new Field(colName, colSchema, "", defaultValue));
            }
        }

        Schema tableSchema = Schema.createRecord(tableRecordName, "", namespacePrefix, false, tableFields);

        // Build envelope schema (nullable unions)
        List<Field> envelopeFields = new ArrayList<>();
        envelopeFields.add(nullableUnionField("beforeImage", tableSchema));
        envelopeFields.add(nullableUnionField("afterImage", tableSchema));
        // Reintroduce metadata fields used by KcopHandler
        envelopeFields.add(nullableUnionField("A_ENTTYP", Schema.create(Type.STRING))); // added back
        envelopeFields.add(nullableUnionField("A_CCID", Schema.create(Type.STRING)));   // added back
        envelopeFields.add(nullableUnionField("A_TIMSTAMP", Schema.create(Type.STRING)));
        // A_JOBUSER/A_USER can be real table columns. Keeping them in the envelope
        // duplicated those fields outside afterImage/beforeImage, so they now stay
        // only where the source metadata says they belong: inside the table image.

        Schema envelopeSchema = Schema.createRecord("AuditRecord", "", namespacePrefix, false, envelopeFields);
        schemaCache.put(tableName, envelopeSchema);
        return envelopeSchema;
    }

    private Field nullableUnionField(String name, Schema nonNullSchema) {
        Schema union = Schema.createUnion(Schema.create(Type.NULL), nonNullSchema);
        return new Field(name, union, "", null);
    }

    private Schema buildColumnSchema(ColumnMetaData cm) {
        String colName = cm.getColumnName();
        ColumnDataType columnDataType = cm.getColumnDataType();
        DataTypes dataTypeEnum = columnDataType != null ? columnDataType.getDataTypeEnum() : null;
        String nativeType = columnDataType != null && columnDataType.getNativeDataType() != null
                ? columnDataType.getNativeDataType().toUpperCase()
                : "";
        String dataTypeRaw = cm.getDataType() != null ? cm.getDataType().toString() : "";
        String dataTypeName = (dataTypeRaw + " " + String.valueOf(dataTypeEnum) + " " + nativeType).toUpperCase();

        int precision = resolvePrecision(cm, columnDataType, dataTypeRaw);
        int scale = resolveScale(cm, columnDataType, dataTypeRaw);
        // Parse patterns like NUMBER(15,2), DECIMAL(10,0), NUMERIC(8)
        Matcher m = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)").matcher(dataTypeRaw);
        if (m.find()) {
            try {
                if (precision <= 0) {
                    precision = Integer.parseInt(m.group(1));
                }
                if (m.group(2) != null) {
                    if (scale < 0) {
                        scale = Integer.parseInt(m.group(2));
                    }
                }
            } catch (NumberFormatException ignore) {
            }
        }

        Schema baseSchema;
        String logicalType;

        if (isFixedPoint(dataTypeEnum, dataTypeName)) {
            // GoldenGate sometimes exposes NUMBER metadata without "(p,s)" in the
            // type text; using ColumnDataType keeps DECIMAL precision as the DB
            // defines it (for example NUMBER(3,0) -> precision 3, not default 15).
            int normalizedScale = scale >= 0 ? scale : 0;
            int normalizedPrecision = precision > 0 ? precision : 15;
            if (normalizedScale == 0 && normalizedPrecision <= 9) {
                baseSchema = Schema.create(Type.INT);
            } else if (normalizedScale == 0 && normalizedPrecision <= 18) {
                baseSchema = Schema.create(Type.LONG);
            } else {
                baseSchema = Schema.create(Type.STRING);
            }
            logicalType = "DECIMAL";
            precision = normalizedPrecision;
            scale = normalizedScale;
        } else if (isInteger(dataTypeEnum, dataTypeName)) {
            if (dataTypeName.contains("BIGINT")) {
                baseSchema = Schema.create(Type.LONG); 
            }else {
                baseSchema = Schema.create(Type.INT);
            }
            logicalType = "DECIMAL";
            precision = precision > 0 ? precision : (baseSchema.getType() == Type.LONG ? 18 : 9);
            scale = scale >= 0 ? scale : 0;
        } else if (isFloatingPoint(dataTypeEnum, dataTypeName)) {
            baseSchema = Schema.create(Type.DOUBLE);
            logicalType = "DOUBLE";
        } else if (isDate(nativeType, dataTypeName)) {
            // Oracle DATE was being classified as TIMESTAMP by the generic
            // DateTime metadata. Native type DATE must stay DATE with length 10.
            baseSchema = Schema.create(Type.STRING);
            logicalType = "DATE";
        } else if (isTimeOrTimestamp(cm, dataTypeName)) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = dataTypeName.contains("TIMESTAMP") ? "TIMESTAMP" : "TIME";
        } else if (dataTypeName.contains("CHAR") || dataTypeName.contains("VARCHAR") || dataTypeName.contains("TEXT")) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "CHARACTER";
        } else if (dataTypeName.contains("BLOB") || dataTypeName.contains("BINARY") || dataTypeName.contains("VARBINARY")) {
            baseSchema = Schema.create(Type.BYTES);
            logicalType = "BINARY";
        } else {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "CHARACTER";
        }

        baseSchema.addProp("logicalType", logicalType);
        baseSchema.addProp("dbColumnName", colName);

        if ("DECIMAL".equals(logicalType)) {
            baseSchema.addProp("precision", precision > 0 ? precision : 15);
            baseSchema.addProp("scale", scale >= 0 ? scale : 0);
        }

        if (logicalType.equals("CHARACTER") || logicalType.equals("TIMESTAMP")
                || logicalType.equals("DATE") || logicalType.equals("TIME")) {
            int length = logicalType.equals("TIMESTAMP") ? 32
                    : logicalType.equals("DATE") ? 10
                    : logicalType.equals("TIME") ? 8 : 255;
            baseSchema.addProp("length", length);
        }

        return baseSchema;
    }

    private boolean isFixedPoint(DataTypes dataTypeEnum, String dataTypeName) {
        return dataTypeEnum == DataTypes.T_FixedPoint
                || dataTypeName.contains("NUMBER")
                || dataTypeName.contains("DECIMAL")
                || dataTypeName.contains("NUMERIC");
    }

    private boolean isInteger(DataTypes dataTypeEnum, String dataTypeName) {
        return dataTypeEnum == DataTypes.T_Integer
                || dataTypeName.contains("INT")
                || dataTypeName.contains("SMALLINT")
                || dataTypeName.contains("BIGINT");
    }

    private boolean isFloatingPoint(DataTypes dataTypeEnum, String dataTypeName) {
        return dataTypeEnum == DataTypes.T_FloatingPoint
                || dataTypeName.contains("FLOAT")
                || dataTypeName.contains("DOUBLE")
                || dataTypeName.contains("REAL");
    }

    private boolean isDate(String nativeType, String dataTypeName) {
        return "DATE".equals(nativeType) || dataTypeName.contains("DATE") && !dataTypeName.contains("TIME");
    }

    private boolean isTimeOrTimestamp(ColumnMetaData cm, String dataTypeName) {
        return dataTypeName.contains("TIME") || cm.isTimestamp() || cm.isTime();
    }

    private int resolvePrecision(ColumnMetaData cm, ColumnDataType columnDataType, String dataTypeRaw) {
        long precision = columnDataType != null ? columnDataType.getPrecision() : -1;
        if (precision <= 0 && cm.getMostSignificantDigit() > 0) {
            precision = cm.getMostSignificantDigit();
        }
        if (precision <= 0) {
            Matcher m = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)").matcher(dataTypeRaw);
            if (m.find()) {
                try {
                    precision = Long.parseLong(m.group(1));
                } catch (NumberFormatException ignore) {
                }
            }
        }
        return precision > 0 && precision <= Integer.MAX_VALUE ? (int) precision : -1;
    }

    private int resolveScale(ColumnMetaData cm, ColumnDataType columnDataType, String dataTypeRaw) {
        int scale = columnDataType != null ? columnDataType.getScale() : -1;
        if (scale < 0 && cm.getLeastSignificantDigit() >= 0) {
            scale = cm.getLeastSignificantDigit();
        }
        if (scale < 0) {
            Matcher m = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)").matcher(dataTypeRaw);
            if (m.find() && m.group(2) != null) {
                try {
                    scale = Integer.parseInt(m.group(2));
                } catch (NumberFormatException ignore) {
                }
            }
        }
        return scale;
    }



    private ColumnMetaData safeGetColumnMetaData(TableMetaData tableMetaData, int index) {
        if (tableMetaData == null || index < 0) return null;
        try {
            return tableMetaData.getColumnMetaData(index);
        } catch (IndexOutOfBoundsException ex) {
            return null;
        }
    }

    public byte[] serializeAvro(Schema schema, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }
}
