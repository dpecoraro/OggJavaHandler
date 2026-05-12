package com.santander.goldengate.handler;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.meta.ColumnDataType;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DataTypes;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;

public class AvroSchemaManagerTest {

    @Test
    void testComputeNamespaceAndEnvelope() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.SOURCEDB", converter);

        // Pass null metadata to avoid GoldenGate dependencies in tests
        Schema envelope = mgr.getOrCreateAvroSchema("ORAPR835.BALP.AEDT074", null);

        assertEquals("value.SOURCEDB", envelope.getNamespace());
        assertEquals("AuditRecord", envelope.getName());

        Field before = envelope.getField("beforeImage");
        Field after = envelope.getField("afterImage");
        assertNotNull(before);
        assertNotNull(after);
        assertEquals(Type.UNION, before.schema().getType());
        assertEquals(Type.UNION, after.schema().getType());

        // union is [null, tableRecord]
        assertEquals(Type.NULL, before.schema().getTypes().get(0).getType());
        Schema tableSchema = before.schema().getTypes().get(1);
        assertEquals(Type.RECORD, tableSchema.getType());
        // No columns when metadata is null
        assertTrue(tableSchema.getFields().isEmpty());
    }

    @Test
    void testSchemaCaching() throws Exception {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB", converter);
        Schema s1 = mgr.getOrCreateAvroSchema("DB.SCH.TBLX", null);
        Schema s2 = mgr.getOrCreateAvroSchema("DB.SCH.TBLX", null);
        assertSame(s1, s2, "Schema should be cached and reused");
    }

    @Test
    void testSerializeAvroNotEmpty() throws Exception {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB", converter);
        Schema envelope = mgr.getOrCreateAvroSchema("DB.SCH.T1", null);

        GenericRecord rec = new GenericData.Record(envelope);
        rec.put("beforeImage", null);
        rec.put("afterImage", null);
        rec.put("A_TIMSTAMP", String.valueOf(System.currentTimeMillis()));

        byte[] bytes = mgr.serializeAvro(envelope, rec);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0, "Serialized Avro payload should not be empty");
    }

    @Test
    void oracleDateUsesDateLogicalTypeAndLengthTen() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB", converter);
        TableMetaData tableMetaData = tableMetaData(dateColumn("DT_PROP", 0));

        Schema envelope = mgr.getOrCreateAvroSchema("DB.SCH.T1", tableMetaData);
        Schema tableSchema = envelope.getField("beforeImage").schema().getTypes().get(1);
        Schema fieldSchema = tableSchema.getField("DT_PROP").schema();

        assertEquals(Type.STRING, fieldSchema.getType());
        assertEquals("DATE", fieldSchema.getProp("logicalType"));
        assertEquals(10, ((Number) fieldSchema.getObjectProp("length")).intValue());
        assertFalse("TIMESTAMP".equalsIgnoreCase(fieldSchema.getProp("logicalType")));
    }

    @Test
    void fixedPointScaleZeroUsesSourcePrecisionAndNumericType() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB", converter);
        TableMetaData tableMetaData = tableMetaData(fixedPointColumn("QT_DIA_ATRS", 0, 3, 0));

        Schema envelope = mgr.getOrCreateAvroSchema("DB.SCH.T1", tableMetaData);
        Schema tableSchema = envelope.getField("beforeImage").schema().getTypes().get(1);
        Schema fieldSchema = tableSchema.getField("QT_DIA_ATRS").schema();

        assertEquals(Type.INT, fieldSchema.getType());
        assertEquals("DECIMAL", fieldSchema.getProp("logicalType"));
        assertEquals(3, ((Number) fieldSchema.getObjectProp("precision")).intValue());
        assertEquals(0, ((Number) fieldSchema.getObjectProp("scale")).intValue());
        assertEquals(0, tableSchema.getField("QT_DIA_ATRS").defaultVal());
    }

    @Test
    void jobUserAndUserStayOnlyInsideTableImageWhenTheyAreSourceColumns() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB", converter);
        TableMetaData tableMetaData = tableMetaData(
                charColumn("A_JOBUSER", 0, 4000),
                charColumn("A_USER", 1, 4000));

        Schema envelope = mgr.getOrCreateAvroSchema("DB.SCH.T1", tableMetaData);
        Schema tableSchema = envelope.getField("afterImage").schema().getTypes().get(1);

        assertNull(envelope.getField("A_JOBUSER"));
        assertNull(envelope.getField("A_USER"));
        assertNotNull(tableSchema.getField("A_JOBUSER"));
        assertNotNull(tableSchema.getField("A_USER"));
    }

    private TableMetaData tableMetaData(ColumnMetaData... columns) {
        return new TableMetaData(new TableName("DB", "SCH", "T1"), java.util.Arrays.asList(columns));
    }

    private ColumnMetaData dateColumn(String name, int index) {
        ColumnDataType type = new ColumnDataType();
        type.setDataType(DataTypes.T_DateTime);
        type.setNativeDataType("DATE");
        type.setColumnLength(10);
        type.setByteSize(10);
        return new ColumnMetaData(name, index, type, true, false, -1, false, -1);
    }

    private ColumnMetaData fixedPointColumn(String name, int index, long precision, int scale) {
        ColumnDataType type = new ColumnDataType();
        type.setDataType(DataTypes.T_FixedPoint);
        type.setNativeDataType("NUMBER");
        type.setPrecision(precision);
        type.setScale(scale);
        type.setColumnLength(precision);
        type.setByteSize(precision);
        return new ColumnMetaData(name, index, type, true, false, -1, false, -1);
    }

    private ColumnMetaData charColumn(String name, int index, long columnLength) {
        ColumnDataType type = new ColumnDataType();
        type.setDataType(DataTypes.T_Character);
        type.setNativeDataType("VARCHAR2");
        type.setColumnLength(columnLength);
        type.setByteSize(columnLength);
        return new ColumnMetaData(name, index, type, true, false, -1, false, -1);
    }
}
