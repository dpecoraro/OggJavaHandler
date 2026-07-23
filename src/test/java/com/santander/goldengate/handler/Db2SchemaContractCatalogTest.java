package com.santander.goldengate.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class Db2SchemaContractCatalogTest {

    @Test
    void loadsAllSchemasFromProductionComparisonReport() {
        Db2SchemaContractCatalog catalog = Db2SchemaContractCatalog.loadBundled();

        assertEquals(165, catalog.keySchemaCount());
        assertEquals(166, catalog.valueSchemaCount());
        assertNotNull(catalog.valueSchema("ORAPR835.BALP.AEDT098"));
        assertNotNull(catalog.keySchema("AEDT074"));
        assertNull(catalog.keySchema("unknown_table"));
    }

    @Test
    void preservesDb2SpecificSmallintTimeAndVarcharContracts() {
        Db2SchemaContractCatalog catalog = Db2SchemaContractCatalog.loadBundled();

        Schema smallintTable = tableRecord(catalog.valueSchema("AEDT098"));
        Schema smallint = smallintTable.getField("QT_PARE").schema();
        assertEquals(Schema.Type.INT, smallint.getType());
        assertEquals("SMALLINT", smallint.getProp("logicalType"));
        assertNull(smallint.getObjectProp("precision"));
        assertNull(smallint.getObjectProp("scale"));

        Schema timeTable = tableRecord(catalog.valueSchema("BGDTCNT"));
        Schema time = timeTable.getField("HORA_CAMSESI").schema();
        assertEquals("TIME", time.getProp("logicalType"));
        assertEquals(8, time.getObjectProp("length"));

        Schema varcharTable = tableRecord(catalog.valueSchema("BGDTMFU"));
        Schema varchar = varcharTable.getField("BGECMIR").schema();
        assertEquals("VARCHAR", varchar.getProp("logicalType"));
        assertEquals(1129, varchar.getObjectProp("length"));
    }

    private Schema tableRecord(Schema envelope) {
        if (envelope.getField("beforeImage") == null) {
            return envelope;
        }
        return envelope.getField("beforeImage").schema().getTypes().stream()
                .filter(schema -> schema.getType() == Schema.Type.RECORD)
                .findFirst()
                .orElseThrow();
    }
}
