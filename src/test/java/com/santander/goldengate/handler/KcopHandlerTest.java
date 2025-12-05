package com.santander.goldengate.handler;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.santander.goldengate.helpers.SchemaTypeConverter;

@Disabled("Depends on Oracle GoldenGate runtime classes not present on test classpath")
public class KcopHandlerTest {

    private KcopHandler handler;
    private AvroSchemaManager schemaManager;

    @BeforeEach
    void setup() {
        handler = new KcopHandler();
        SchemaTypeConverter converter = new SchemaTypeConverter();
        schemaManager = new AvroSchemaManager("value.TEST.DB", converter);
        try {
            Field f = KcopHandler.class.getDeclaredField("schemaManager");
            f.setAccessible(true);
            f.set(handler, schemaManager);
        } catch (Exception e) {
            fail("Failed to inject schemaManager: " + e.getMessage());
        }
    }

    @Test
    void testResolveTopicTemplate() {
        String template = "BR.CDC.ORA.TS.${fullyQualifiedTableName}";
        String topic = handler.resolveTopic(template, "ORAPR835.BALP.AEDT074");
        assertEquals("BR.CDC.ORA.TS.ORAPR835.BALP.AEDT074", topic);
    }

    @Test
    void testResolveTopicFallback() {
        String topic = handler.resolveTopic(null, "ORAPR835.BALP.AEDT074");
        assertEquals("cdc.orapr835_balp_aedt074", topic);
    }

    @Test
    void testBuildKeyUnknown() {
        String key = handler.buildKey(null);
        assertEquals("unknown", key);
    }

    @Test
    void testExtractValueBytesToBase64() {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        Object out = handler.extractValue(bytes);
        assertTrue(out instanceof String);
        assertEquals(Base64.getEncoder().encodeToString(bytes), out);
    }

    @Test
    void testExtractValuePassThrough() {
        assertEquals("abc", handler.extractValue("abc"));
        assertEquals(10L, handler.extractValue(10L));
    }

    @Test
    void testConvertInt() {
        Schema intSchema = Schema.create(Type.INT);
        assertEquals(42, handler.convertValueToSchemaType("42", intSchema));
        assertEquals(7, handler.convertValueToSchemaType(7.99, intSchema)); // truncates
    }

    @Test
    void testConvertLong() {
        Schema longSchema = Schema.create(Type.LONG);
        assertEquals(9999999999L, handler.convertValueToSchemaType("9999999999", longSchema));
        // decimal string: rounds
        assertEquals(17000L, handler.convertValueToSchemaType("17000.00", longSchema));
        assertEquals(66501L, handler.convertValueToSchemaType("66500.51", longSchema));
    }

    @Test
    void testConvertDouble() {
        Schema dbl = Schema.create(Type.DOUBLE);
        assertEquals(123.45, (Double) handler.convertValueToSchemaType("123.45", dbl), 0.00001);
        assertEquals(10.0, (Double) handler.convertValueToSchemaType(10, dbl), 0.00001);
    }

    @Test
    void testConvertString() {
        Schema str = Schema.create(Type.STRING);
        assertEquals("xpto", handler.convertValueToSchemaType("xpto", str));
        assertEquals("10", handler.convertValueToSchemaType(10, str));
    }

    @Test
    void testConvertBytesFromByteArray() {
        Schema bytesSchema = Schema.create(Type.BYTES);
        byte[] src = new byte[] {5, 6, 7};
        Object out = handler.convertValueToSchemaType(src, bytesSchema);
        assertTrue(out instanceof ByteBuffer);
        ByteBuffer bb = (ByteBuffer) out;
        byte[] got = new byte[bb.remaining()];
        bb.get(got);
        assertArrayEquals(src, got);
    }

    @Test
    void testConvertBytesFromBase64String() {
        Schema bytesSchema = Schema.create(Type.BYTES);
        byte[] src = new byte[] {9, 8, 7, 6};
        String base64 = Base64.getEncoder().encodeToString(src);
        Object out = handler.convertValueToSchemaType(base64, bytesSchema);
        assertTrue(out instanceof ByteBuffer);
        ByteBuffer bb = (ByteBuffer) out;
        byte[] got = new byte[bb.remaining()];
        bb.get(got);
        assertArrayEquals(src, got);
    }

    @Test
    void testConvertBytesFromPlainStringFallback() {
        Schema bytesSchema = Schema.create(Type.BYTES);
        String plain = "hello";
        Object out = handler.convertValueToSchemaType(plain, bytesSchema);
        assertTrue(out instanceof ByteBuffer);
        ByteBuffer bb = (ByteBuffer) out;
        byte[] got = new byte[bb.remaining()];
        bb.get(got);
        assertArrayEquals(plain.getBytes(), got);
    }
}