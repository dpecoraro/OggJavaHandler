package com.santander.goldengate.handler;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import com.santander.goldengate.helpers.ColumnSchemaMapper;
import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

/**
 * Manages Avro schema creation and caching for GoldenGate data replication.
 */
public class AvroSchemaManager {

    private final Map<String, Schema> schemaCache = new HashMap<>();
    private final String namespacePrefix;
    private final ColumnSchemaMapper columnSchemaMapper;

    public AvroSchemaManager(String namespacePrefix, SchemaTypeConverter schemaTypeConverter) {
        this(namespacePrefix, new ColumnSchemaMapper());
    }

    AvroSchemaManager(String namespacePrefix, ColumnSchemaMapper columnSchemaMapper) {
        this.namespacePrefix = namespacePrefix;
        this.columnSchemaMapper = columnSchemaMapper;
    }

    public Schema getOrCreateAvroSchema(String tableName, TableMetaData tableMetaData) {
        Schema cached = schemaCache.get(tableName);
        if (cached != null) return cached;

        final String rawName = tableName != null && tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1)
                : tableName;
        final String tableRecordName = rawName != null ? rawName.toUpperCase() : "UNKNOWN";
        // Build table record schema
        List<Field> tableFields = new ArrayList<>();

        // Always iterate using safeGetColumnMetaData to cover all columns
        if (tableMetaData != null) {
            for (int idx = 0; ; idx++) {
                ColumnMetaData cm = safeGetColumnMetaData(tableMetaData, idx);
                if (cm == null) break;
                String colName = cm.getColumnName();
                if (isEnvelopeAuditOnlyField(colName)) {
                    // O CDC original mantém estes campos somente no envelope.
                    // O GoldenGate os expõe na metadata da tabela, mas incluí-los
                    // aqui duplicava A_JOBUSER/A_USER dentro de before/afterImage.
                    continue;
                }
                ColumnSchemaMapper.Mapping mapping = columnSchemaMapper.map(cm);
                tableFields.add(new Field(
                        colName,
                        mapping.getSchema(),
                        "",
                        mapping.getDefaultValue()));
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

    public void clearCache() {
        schemaCache.clear();
    }

    private Field nullableUnionField(String name, Schema nonNullSchema) {
        Schema union = Schema.createUnion(Schema.create(Type.NULL), nonNullSchema);
        return new Field(name, union, "", null);
    }

    private boolean isEnvelopeAuditOnlyField(String colName) {
        return "A_JOBUSER".equalsIgnoreCase(colName) || "A_USER".equalsIgnoreCase(colName);
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
