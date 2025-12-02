package com.santander.goldengate.handler;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class AvroSchemaManagerTest {

    @Test
    void testComputeNamespaceAndEnvelope() {
        AvroSchemaManager mgr = new AvroSchemaManager("value.SOURCEDB");

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
    void testSchemaCaching() {
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB");
        Schema s1 = mgr.getOrCreateAvroSchema("DB.SCH.TBLX", null);
        Schema s2 = mgr.getOrCreateAvroSchema("DB.SCH.TBLX", null);
        assertSame(s1, s2, "Schema should be cached and reused");
    }

    @Test
    void testSerializeAvroNotEmpty() throws Exception {
        AvroSchemaManager mgr = new AvroSchemaManager("value.DB");
        Schema envelope = mgr.getOrCreateAvroSchema("DB.SCH.T1", null);

        GenericRecord rec = new GenericData.Record(envelope);
        rec.put("beforeImage", null);
        rec.put("afterImage", null);
        rec.put("A_TIMSTAMP", String.valueOf(System.currentTimeMillis()));
        rec.put("A_JOBUSER", "tester");
        rec.put("A_USER", "tester");

        byte[] bytes = mgr.serializeAvro(envelope, rec);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0, "Serialized Avro payload should not be empty");
    }
}