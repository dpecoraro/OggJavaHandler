package com.santander.goldengate.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;

import oracle.goldengate.datasource.meta.ColumnDataType;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DataTypes;

class ColumnSchemaMapperTest {
    private final ColumnSchemaMapper mapper = new ColumnSchemaMapper();

    @Test
    void mapsDecimalBoundaryTypesAndTypedDefaults() {
        assertDecimal(9, 0, Type.INT, 0);
        assertDecimal(10, 0, Type.LONG, 0);
        assertDecimal(18, 0, Type.LONG, 0);
        assertDecimal(19, 0, Type.STRING, "0");
        assertDecimal(8, 2, Type.STRING, "0");
    }

    @Test
    void mapsSmallintDateTimestampAndCharacterContract() {
        ColumnSchemaMapper.Mapping smallint = mapper.map(column(
                "QT_ITEM", DataTypes.T_Integer, "SMALLINT", 5, 0, 2));
        assertEquals(Type.INT, smallint.getSchema().getType());
        assertEquals("SMALLINT", smallint.getSchema().getProp("logicalType"));
        assertEquals(null, smallint.getSchema().getObjectProp("precision"));
        assertEquals(null, smallint.getSchema().getObjectProp("scale"));
        assertEquals(0, smallint.getDefaultValue());

        ColumnSchemaMapper.Mapping date = mapper.map(column(
                "DT_EVENTO", DataTypes.T_DateTime, "TIMESTAMP", 0, 0, 32));
        assertEquals("DATE", date.getSchema().getProp("logicalType"));
        assertEquals(10, date.getSchema().getObjectProp("length"));

        ColumnSchemaMapper.Mapping timestamp = mapper.map(column(
                "DH_EVENTO", DataTypes.T_DateTime, "TIMESTAMP", 0, 0, 32));
        assertEquals("TIMESTAMP", timestamp.getSchema().getProp("logicalType"));
        assertEquals(32, timestamp.getSchema().getObjectProp("length"));

        ColumnSchemaMapper.Mapping time = mapper.map(column(
                "HR_EVENTO", DataTypes.T_DateTime, "TIME", 0, 0, 8));
        assertEquals("TIME", time.getSchema().getProp("logicalType"));
        assertEquals(8, time.getSchema().getObjectProp("length"));

        ColumnSchemaMapper.Mapping character = mapper.map(column(
                "CD_TIPO", DataTypes.T_Character, "CHAR", 0, 0, 3));
        assertEquals(3, character.getSchema().getObjectProp("length"));
    }

    @Test
    void rejectsMissingDecimalPrecisionAndCharacterLength() {
        assertThrows(IllegalArgumentException.class,
                () -> mapper.map(column("VL", DataTypes.T_FixedPoint, "DECIMAL", 0, 2, 8)));
        assertThrows(IllegalArgumentException.class,
                () -> mapper.map(column("CD", DataTypes.T_Character, "CHAR", 0, 0, 0)));
    }

    private void assertDecimal(int precision, int scale, Type expectedType, Object expectedDefault) {
        ColumnSchemaMapper.Mapping mapping = mapper.map(column(
                "VL_" + precision + "_" + scale,
                DataTypes.T_FixedPoint,
                "DECIMAL",
                precision,
                scale,
                precision));
        assertEquals(expectedType, mapping.getSchema().getType());
        assertEquals("DECIMAL", mapping.getSchema().getProp("logicalType"));
        assertEquals(precision, mapping.getSchema().getObjectProp("precision"));
        assertEquals(scale, mapping.getSchema().getObjectProp("scale"));
        assertEquals(expectedDefault, mapping.getDefaultValue());
    }

    private ColumnMetaData column(
            String name,
            DataTypes dataType,
            String nativeType,
            long precision,
            int scale,
            long length) {
        ColumnDataType type = new ColumnDataType();
        type.setDataType(dataType);
        type.setNativeDataType(nativeType);
        type.setPrecision(precision);
        type.setScale(scale);
        type.setColumnLength(length);
        type.setByteSize(length);
        return new ColumnMetaData(name, 0, type, true, false, -1, false, -1);
    }
}
