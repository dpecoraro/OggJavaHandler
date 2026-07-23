package com.santander.goldengate.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;

import oracle.goldengate.datasource.meta.ColumnDataType;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DataTypes;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;

class SchemaTypeConverterTest {

    @Test
    void cloneRecordWithCharLengthsUsesDeclaredLogicalLengthWithoutUtf8Heuristic() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        TableMetaData tableMetaData = tableMetaData(column("CD_BANC", 0, 12L));

        Schema original = tableRecord("AEDT074", field("CD_BANC", 255));
        Schema cloned = converter.cloneRecordWithCharLengths(original, tableMetaData);

        Schema.Field clonedField = cloned.getField("CD_BANC");
        Object length = clonedField.schema().getObjectProp("length");
        String clonedJson = cloned.toString(true);

        assertInstanceOf(Number.class, length);
        assertEquals(12, ((Number) length).intValue());
        assertEquals("", clonedField.doc());
        assertFalse(clonedField.hasDefaultValue());
        assertTrue(clonedJson.contains("\"length\" : 12"));
        assertFalse(clonedJson.contains("\"length\" : \"12\""));
    }

    @Test
    void rebuildEnvelopeWithClonedTableSchemaDoesNotInjectNullDefaults() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        TableMetaData tableMetaData = tableMetaData(column("CD_BANC", 0, 12L));

        Schema table = tableRecord("AEDT074", field("CD_BANC", 255));
        Schema envelope = envelopeRecord(table);

        Schema rebuilt = converter.rebuildEnvelopeWithClonedTableSchema(envelope, tableMetaData);
        String rebuiltJson = rebuilt.toString(true);

        assertFalse(rebuilt.getField("beforeImage").hasDefaultValue());
        assertFalse(rebuilt.getField("afterImage").hasDefaultValue());
        assertFalse(rebuilt.getField("A_ENTTYP").hasDefaultValue());
        assertFalse(rebuiltJson.contains("\"default\""));
        assertTrue(rebuiltJson.contains("\"doc\" : \"\""));
    }

    @Test
    void rebuildEnvelopeWithClonedTableSchemaPreservesFieldOrder() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        TableMetaData tableMetaData = tableMetaData(
                column("CD_BANC", 0, 12L),
                column("CD_CENT_CPTU", 1, 12L),
                column("AN_PROP", 2, 12L),
                column("NR_SOLI", 3, 24L));

        Schema table = tableRecord(
                "AEDT074",
                field("CD_BANC", 255),
                field("CD_CENT_CPTU", 255),
                field("AN_PROP", 255),
                field("NR_SOLI", 255));
        Schema envelope = envelopeRecord(table);

        Schema rebuilt = converter.rebuildEnvelopeWithClonedTableSchema(envelope, tableMetaData);
        Schema rebuiltTable = rebuilt.getField("beforeImage").schema().getTypes().get(1);
        List<String> names = rebuiltTable.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());

        assertIterableEquals(Arrays.asList("CD_BANC", "CD_CENT_CPTU", "AN_PROP", "NR_SOLI"), names);
    }

    private Schema envelopeRecord(Schema tableSchema) {
        return Schema.createRecord(
                "AuditRecord",
                "",
                "value.SOURCEDB.BALP",
                false,
                Arrays.asList(
                        nullableUnionField("beforeImage", tableSchema),
                        nullableUnionField("afterImage", tableSchema),
                        nullableUnionField("A_ENTTYP", Schema.create(Type.STRING))));
    }

    private Schema.Field nullableUnionField(String name, Schema nonNullSchema) {
        Schema union = Schema.createUnion(Schema.create(Type.NULL), nonNullSchema);
        return new Schema.Field(name, union, "", null);
    }

    private Schema tableRecord(String name, Schema.Field... fields) {
        return Schema.createRecord(name, "", "value.SOURCEDB.BALP", false, Arrays.asList(fields));
    }

    private Schema.Field field(String name, int length) {
        Schema schema = Schema.create(Type.STRING);
        schema.addProp("logicalType", "CHARACTER");
        schema.addProp("dbColumnName", name);
        schema.addProp("length", length);
        return new Schema.Field(name, schema, "", null);
    }

    private TableMetaData tableMetaData(ColumnMetaData... columns) {
        return new TableMetaData(new TableName("DB", "SCH", "AEDT074"), Arrays.asList(columns));
    }

    private ColumnMetaData column(String name, int index, long columnLength) {
        ColumnDataType type = new ColumnDataType();
        type.setDataType(DataTypes.T_Character);
        type.setColumnLength(columnLength);
        type.setByteSize(columnLength);
        type.setNativeDataType("VARCHAR2");
        return new ColumnMetaData(name, index, type, true, false, -1, false, -1);
    }
}
