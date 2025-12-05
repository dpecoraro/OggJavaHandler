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

import oracle.goldengate.datasource.meta.ColumnMetaData;
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

        final String tableRecordName = tableName != null && tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1)
                : tableName;

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
        envelopeFields.add(nullableUnionField("A_JOBUSER", Schema.create(Type.STRING)));
        envelopeFields.add(nullableUnionField("A_USER", Schema.create(Type.STRING)));

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
        String dataTypeName = cm.getDataType() != null ? cm.getDataType().toString().toUpperCase() : "STRING";
        String dataTypeRaw = cm.getDataType() != null ? cm.getDataType().toString() : "";

        int precision = -1;
        int scale = -1;
        // Parse patterns like NUMBER(15,2), DECIMAL(10,0), NUMERIC(8)
        Matcher m = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)").matcher(dataTypeRaw);
        if (m.find()) {
            try {
                precision = Integer.parseInt(m.group(1));
                if (m.group(2) != null) {
                    scale = Integer.parseInt(m.group(2));
                }
            } catch (NumberFormatException ignore) {
            }
        }

        Schema baseSchema;
        String logicalType;

        if (dataTypeName.contains("NUMBER") || dataTypeName.contains("DECIMAL") || dataTypeName.contains("NUMERIC")) {
            if (scale > 0) {
                baseSchema = Schema.create(Type.DOUBLE);
            } else {
                baseSchema = Schema.create(Type.LONG);
            }
            logicalType = "DECIMAL";
        } else if (dataTypeName.contains("INT") || dataTypeName.contains("SMALLINT") || dataTypeName.contains("BIGINT")) {
            if (dataTypeName.contains("BIGINT")) {
                baseSchema = Schema.create(Type.LONG); 
            }else {
                baseSchema = Schema.create(Type.INT);
            }
            logicalType = "DECIMAL";
        } else if (dataTypeName.contains("FLOAT") || dataTypeName.contains("DOUBLE") || dataTypeName.contains("REAL")) {
            baseSchema = Schema.create(Type.DOUBLE);
            logicalType = "DOUBLE";
        } else if (dataTypeName.contains("DATE")) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "DATE";
        } else if (dataTypeName.contains("TIME")) {
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
