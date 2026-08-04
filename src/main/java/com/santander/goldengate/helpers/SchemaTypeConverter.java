package com.santander.goldengate.helpers;

import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

public class SchemaTypeConverter {
    public Object getDefaultValue(Schema schema) {
        Type type = schema.getType();
        String logicalType = schema.getProp("logicalType");
        if ("DECIMAL".equalsIgnoreCase(logicalType)) {
            return type == Type.STRING ? "0" : 0;
        }
        if ("SMALLINT".equalsIgnoreCase(logicalType)
                || "INTEGER".equalsIgnoreCase(logicalType)) {
            return 0;
        }
        switch (type) {
            case INT:
            case LONG: return 0;
            case FLOAT:
            case DOUBLE: return 0.0;
            case BOOLEAN: return false;
            case STRING: return "";
            case BYTES: return java.nio.ByteBuffer.wrap(new byte[0]);
            default: return null;
        }
    }

    public Schema nonNullSchema(Schema schema) {
        if (schema != null && schema.getType() == Type.UNION) {
            return schema.getTypes().stream()
                    .filter(candidate -> candidate.getType() != Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }

    public boolean allowsNull(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.getType() == Type.NULL) {
            return true;
        }
        return schema.getType() == Type.UNION
                && schema.getTypes().stream().anyMatch(candidate -> candidate.getType() == Type.NULL);
    }

    public Schema cloneRecordWithCharLengths(Schema record, TableMetaData tmd) {
        java.util.List<Schema.Field> clonedFields = new java.util.ArrayList<>();

        for (Schema.Field f : record.getFields()) {
            Schema fs = f.schema();

            // se for UNION (null + tipo), trate o “tipo real”
            Schema effective = fs;
            if (fs.getType() == Schema.Type.UNION) {
                effective = fs.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst().orElse(fs);
            }

            Schema newEffective = effective;

            String logical = effective.getProp("logicalType");

            if (effective.getType() == Schema.Type.STRING
                    && "CHARACTER".equalsIgnoreCase(logical)) {

                ColumnMetaData col = findColumnByName(tmd, f.name());

                int charLen = existingLength(effective);
                if (col != null && col.getColumnDataType() != null
                        && col.getColumnDataType().getColumnLength() > 0) {
                    charLen = Math.toIntExact(col.getColumnDataType().getColumnLength());
                } else if (col != null && col.getColumnLength() > 0) {
                    charLen = Math.toIntExact(col.getColumnLength());
                }
                if (charLen <= 0) {
                    throw new IllegalArgumentException("Missing character length for column " + f.name());
                }

                Schema s2 = Schema.create(Schema.Type.STRING);

                copySchemaPropsExcept(effective, s2, "length");

                s2.addProp("length", charLen);

                newEffective = s2;
            }

            Schema newFieldSchema = fs;
            if (fs.getType() == Schema.Type.UNION) {
                newFieldSchema = replaceNonNullInUnion(fs, newEffective);
            } else {
                newFieldSchema = newEffective;
            }

            clonedFields.add(copyField(f, newFieldSchema));
        }

        Schema out = Schema.createRecord(record.getName(), record.getDoc(), record.getNamespace(), record.isError(), clonedFields);
        copyRecordProps(record, out);
        copySchemaAliases(record, out);
        return out;
    }

    private int existingLength(Schema schema) {
        Object length = schema.getObjectProp("length");
        return length instanceof Number ? ((Number) length).intValue() : -1;
    }

    public void copySchemaPropsExcept(Schema from, Schema to, String... excluded) {
        if (from == null || to == null) {
            return;
        }

        java.util.Set<String> ex = new java.util.HashSet<>(java.util.Arrays.asList(excluded));

        for (Map.Entry<String, Object> e : from.getObjectProps().entrySet()) {
            if (e.getKey() == null) {
                continue;
            }
            if (ex.contains(e.getKey())) {
                continue;
            }
            if (e.getValue() != null) {
                to.addProp(e.getKey(), e.getValue());
            }
        }
    }

    private void copySchemaProps(Schema from, Schema to) {
        if (from == null || to == null) {
            return;
        }
        for (Map.Entry<String, Object> e : from.getObjectProps().entrySet()) {
            if (e.getValue() != null) {
                to.addProp(e.getKey(), e.getValue());
            }
        }
    }

    private void copyProps(Schema.Field from, Schema.Field to) {
        for (Map.Entry<String, Object> e : from.getObjectProps().entrySet()) {
            if (e.getValue() != null) {
                to.addProp(e.getKey(), e.getValue());
            }
        }
    }

    private void copyRecordProps(Schema from, Schema to) {
        copySchemaProps(from, to);
    }

    private Schema replaceNonNullInUnion(Schema union, Schema newNonNull) {
        java.util.List<Schema> types = new java.util.ArrayList<>();
        for (Schema s : union.getTypes()) {
            if (s.getType() == Schema.Type.NULL) {
                types.add(s);
            } else {
                types.add(newNonNull);
            }
        }
        return Schema.createUnion(types);
    }

    public Schema rebuildEnvelopeWithClonedTableSchema(Schema envelope, TableMetaData tmd) {
        if (envelope == null || tmd == null) {
            return envelope;
        }

        Schema tableRecord = extractTableRecordSchema(envelope);
        if (tableRecord == null) {
            return envelope;
        }

        Schema clonedTable = cloneRecordWithCharLengths(tableRecord, tmd);

        // agora recria o envelope record trocando o tipo dos campos beforeImage/afterImage
        java.util.List<Schema.Field> rebuiltFields = new java.util.ArrayList<>();

        for (Schema.Field f : envelope.getFields()) {
            if ("beforeImage".equals(f.name()) || "afterImage".equals(f.name())) {
                Schema newFieldSchema = replaceRecordInsideUnion(f.schema(), clonedTable);
                rebuiltFields.add(copyField(f, newFieldSchema));
            } else {
                rebuiltFields.add(copyField(f, f.schema()));
            }
        }

        Schema rebuilt = Schema.createRecord(
                envelope.getName(),
                envelope.getDoc(),
                envelope.getNamespace(),
                envelope.isError(),
                rebuiltFields);
        copyRecordProps(envelope, rebuilt);
        copySchemaAliases(envelope, rebuilt);
        return rebuilt;
    }

    private Schema replaceRecordInsideUnion(Schema original, Schema newRecord) {
        if (original.getType() != Schema.Type.UNION) {
            return newRecord;
        }
        java.util.List<Schema> types = new java.util.ArrayList<>();
        for (Schema s : original.getTypes()) {
            if (s.getType() == Schema.Type.RECORD) {
                types.add(newRecord);
            } else {
                types.add(s); // null, etc.

            }
        }
        return Schema.createUnion(types);
    }
    private Schema extractTableRecordSchema(Schema envelopeSchema) {
        if (envelopeSchema == null) {
            return null;
        }
        //System.out.println(">>> [KcopHandler] Extracting table record schema from envelope: " + envelopeSchema.getFullName());
        Schema.Field before = envelopeSchema.getField("beforeImage");
        if (before != null) {
            Schema s = before.schema();
            if (s.getType() == Type.UNION) {
                for (Schema t : s.getTypes()) {
                    if (t.getType() == Type.RECORD) {
                        return t;
                    }
                }
            } else if (s.getType() == Type.RECORD) {
                return s;
            }
        }
        Schema.Field after = envelopeSchema.getField("afterImage");
        if (after != null) {
            Schema s = after.schema();
            if (s.getType() == Type.UNION) {
                for (Schema t : s.getTypes()) {
                    if (t.getType() == Type.RECORD) {
                        return t;
                    }
                }
            } else if (s.getType() == Type.RECORD) {
                return s;
            }
        }
        return null;
    }

    public ColumnMetaData findColumnByName(TableMetaData tmd, String name) {
        if (tmd == null || name == null) {
            return null;
        }
        String target = name.toUpperCase();
        for (int i = 0; i < tmd.getNumColumns(); i++) {
            ColumnMetaData col = tmd.getColumnMetaData(i);
            if (col != null && target.equals(col.getColumnName().toUpperCase())) {
                return col;
            }
        }
        return null;
    }

    private Schema.Field copyField(Schema.Field from, Schema newSchema) {
        Schema.Field copy = new Schema.Field(
                from.name(),
                newSchema,
                from.doc(),
                resolveDefaultValue(from),
                from.order());
        copyProps(from, copy);
        copyFieldAliases(from, copy);
        return copy;
    }

    private Object resolveDefaultValue(Schema.Field field) {
        if (!field.hasDefaultValue()) {
            return null;
        }
        Object defaultValue = field.defaultVal();
        return defaultValue == JsonProperties.NULL_VALUE ? Schema.Field.NULL_DEFAULT_VALUE : defaultValue;
    }

    private void copySchemaAliases(Schema from, Schema to) {
        for (String alias : from.getAliases()) {
            to.addAlias(alias);
        }
    }

    private void copyFieldAliases(Schema.Field from, Schema.Field to) {
        for (String alias : from.aliases()) {
            to.addAlias(alias);
        }
    }

}
