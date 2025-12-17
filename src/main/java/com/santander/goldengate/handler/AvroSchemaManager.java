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
import org.apache.avro.SchemaBuilder;
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

    public Schema getOrCreateAvroSchema(String fullyQualifiedTableName, TableMetaData tableMetaData) {
        Schema cached = schemaCache.get(fullyQualifiedTableName);
        if (cached != null) return cached;

        final String tableRecordName = fullyQualifiedTableName != null && fullyQualifiedTableName.contains(".")
                ? fullyQualifiedTableName.substring(fullyQualifiedTableName.lastIndexOf('.') + 1)
                : fullyQualifiedTableName;

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

        // CHANGED: build the inner table record schema via our mapper (DECIMAL/DATE/TIMESTAMP handling)
        Schema tableSchema = buildTableSchemaForColumns(tableMetaData, fullyQualifiedTableName);

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
        schemaCache.put(fullyQualifiedTableName, envelopeSchema);
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
            baseSchema = Schema.create(Type.STRING);
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

    // Build the table (record) schema from metadata (DECIMAL->STRING with scale>=2; DATE->string; TIMESTAMP->string)
    private Schema buildTableSchemaForColumns(TableMetaData tableMetaData, String fullyQualifiedTableName) {
        // derive a safe short record name (fallback when metadata does not expose name)
        String shortName = resolveShortName(tableMetaData, fullyQualifiedTableName);

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
            .record(shortName)
            .namespace(namespacePrefix)
            .fields();

        int cols = (tableMetaData != null ? tableMetaData.getNumColumns() : 0);
        for (int i = 0; i < cols; i++) {
            ColumnMetaData col = tableMetaData.getColumnMetaData(i);
            if (col == null) continue;

            String colName = col.getColumnName();
            String dt = col.getDataType() != null ? col.getDataType().toString().toUpperCase() : "";

            // DECIMAL/NUMBER -> STRING with logicalType=DECIMAL and scale (default 2 if <=0)
            if (isDecimalLike(dt)) {
                Schema strDecimal = Schema.create(Schema.Type.STRING);
                strDecimal.addProp("logicalType", "DECIMAL");
                strDecimal.addProp("dbColumnName", colName);
                int precision = safeGetPrecision(col, 38);
                int scale = safeGetScale(col, 2);
                if (scale <= 0) scale = 2;
                strDecimal.addProp("precision", precision);
                strDecimal.addProp("scale", scale);

                fields.name(colName).type(strDecimal).withDefault("");
                continue;
            }

            // DATE-like (DT_* or DATE datatype) -> STRING with logicalType=DATE
            if ((colName != null && colName.toUpperCase().startsWith("DT_")) || dt.contains("DATE")) {
                Schema dateStr = Schema.create(Schema.Type.STRING);
                dateStr.addProp("logicalType", "DATE");
                dateStr.addProp("dbColumnName", colName);
                dateStr.addProp("length", 10);
                fields.name(colName).type(dateStr).withDefault("");
                continue;
            }

            // TIMESTAMP-like -> STRING with logicalType=TIMESTAMP
            if (dt.contains("TIMESTAMP") || dt.contains("TIME")) {
                Schema tsStr = Schema.create(Schema.Type.STRING);
                tsStr.addProp("logicalType", "TIMESTAMP");
                tsStr.addProp("dbColumnName", colName);
                tsStr.addProp("length", 32);
                fields.name(colName).type(tsStr).withDefault("");
                continue;
            }

            // CHARACTER/default -> STRING with logicalType=CHARACTER
            Schema str = Schema.create(Schema.Type.STRING);
            str.addProp("logicalType", "CHARACTER");
            str.addProp("dbColumnName", colName);
            fields.name(colName).type(str).withDefault("");
        }

        return fields.endRecord();
    }

    // Resolve table short name from metadata or FQTN; fallback to "table_record"
    private String resolveShortName(TableMetaData meta, String fullyQualifiedTableName) {
        try {
            if (meta != null) {
                // Try: meta.getTableName().getShortName()
                Object tn = meta.getClass().getMethod("getTableName").invoke(meta);
                if (tn != null) {
                    try {
                        Object s = tn.getClass().getMethod("getShortName").invoke(tn);
                        if (s instanceof String && !((String) s).isEmpty()) {
                            return ((String) s).toString().toLowerCase();
                        }
                    } catch (Exception ignore) {}
                    // Fallback: use toString and take last token after '.'
                    String full = tn.toString();
                    if (full != null && !full.isEmpty()) {
                        int idx = full.lastIndexOf('.');
                        String last = (idx >= 0 ? full.substring(idx + 1) : full);
                        if (!last.isEmpty()) return last.toLowerCase();
                    }
                }
            }
        } catch (Exception ignore) {}
        // fallback from fullyQualifiedTableName
        if (fullyQualifiedTableName != null && !fullyQualifiedTableName.isEmpty()) {
            int idx = fullyQualifiedTableName.lastIndexOf('.');
            String last = (idx >= 0 ? fullyQualifiedTableName.substring(idx + 1) : fullyQualifiedTableName);
            if (!last.isEmpty()) return last.toLowerCase();
        }
        return "table_record";
    }

    // Helpers for DECIMAL detection/metadata access. Reuse existing ones if already present.
    private boolean isDecimalLike(String dtUpper) {
        return dtUpper != null && (dtUpper.contains("NUMBER") || dtUpper.contains("DECIMAL") || dtUpper.contains("NUMERIC"));
    }

    private int safeGetScale(ColumnMetaData col, int def) {
        String[] getters = { "getScale", "getColumnScale", "getFractionalDigits", "getDecimalDigits" };
        for (String m : getters) {
            try {
                Object v = col.getClass().getMethod(m).invoke(col);
                if (v instanceof Number) return ((Number) v).intValue();
            } catch (Exception ignore) {}
        }
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('('), r = dt.indexOf(')');
                if (l >= 0 && r > l) {
                    String[] parts = dt.substring(l + 1, r).split(",");
                    if (parts.length == 2) return Integer.parseInt(parts[1].trim());
                }
            }
        } catch (Exception ignore) {}
        return def;
    }

    private int safeGetPrecision(ColumnMetaData col, int def) {
        String[] getters = { "getPrecision", "getColumnPrecision", "getLength", "getDisplaySize" };
        for (String m : getters) {
            try {
                Object v = col.getClass().getMethod(m).invoke(col);
                if (v instanceof Number) return Math.max(1, ((Number) v).intValue());
            } catch (Exception ignore) {}
        }
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('('), r = dt.indexOf(')');
                if (l >= 0 && r > l) {
                    String[] parts = dt.substring(l + 1, r).split(",");
                    if (parts.length >= 1) return Math.max(1, Integer.parseInt(parts[0].trim()));
                }
            }
        } catch (Exception ignore) {}
        return def;
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
