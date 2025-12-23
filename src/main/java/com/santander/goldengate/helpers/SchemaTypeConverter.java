package com.santander.goldengate.helpers;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;

import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

public class SchemaTypeConverter {
    public Object getDefaultValue(Schema schema) {
        Type type = schema.getType();
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

        public Schema cloneRecordWithCharLengths(Schema record, TableMetaData tmd) {
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder
                .record(record.getName())
                .namespace(record.getNamespace())
                .fields();

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

                int byteLen = 255;
                int charLen = 255;
                if (col != null) {
                    try {
                        Method m = col.getClass().getMethod("getColumnLength");
                        Object v = m.invoke(col);
                        if (v instanceof Number && ((Number) v).intValue() > 0) {
                            byteLen = ((Number) v).intValue();

                            // 🔥 heurística UTF-8 (3 bytes por char)
                            if (byteLen % 3 == 0) {
                                charLen = byteLen / 3;
                            } else {
                                charLen = byteLen; // fallback defensivo
                            }
                        }
                    } catch (Exception ignore) {
                    }
                }

                Schema s2 = Schema.create(Schema.Type.STRING);

                // copia tudo MENOS length antigo
                copySchemaPropsExcept(effective, s2, "length");

                s2.addProp("length", String.valueOf(charLen));

                newEffective = s2;
            }

            Schema newFieldSchema = fs;
            if (fs.getType() == Schema.Type.UNION) {
                newFieldSchema = replaceNonNullInUnion(fs, newEffective);
            } else {
                newFieldSchema = newEffective;
            }

            Schema.Field nf = new Schema.Field(f.name(), newFieldSchema, f.doc(), f.defaultVal());
            copyProps(f, nf);
            fa = fa.name(nf.name()).type(nf.schema()).withDefault(nf.defaultVal());
        }

        Schema out = fa.endRecord();
        copyRecordProps(record, out);
        return out;
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
                to.addProp(e.getKey(), String.valueOf(e.getValue()));
            }
        }
    }

    private void copySchemaProps(Schema from, Schema to) {
        if (from == null || to == null) {
            return;
        }
        for (Map.Entry<String, Object> e : from.getObjectProps().entrySet()) {
            if (e.getValue() != null) {
                to.addProp(e.getKey(), String.valueOf(e.getValue()));
            }
        }
    }

    private void copyProps(Schema.Field from, Schema.Field to) {
        for (Map.Entry<String, Object> e : from.getObjectProps().entrySet()) {
            if (e.getValue() != null) {
                to.addProp(e.getKey(), String.valueOf(e.getValue()));
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
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder
                .record(envelope.getName())
                .namespace(envelope.getNamespace())
                .fields();

        for (Schema.Field f : envelope.getFields()) {
            if ("beforeImage".equals(f.name()) || "afterImage".equals(f.name())) {
                Schema newFieldSchema = replaceRecordInsideUnion(f.schema(), clonedTable);
                Schema.Field nf = new Schema.Field(f.name(), newFieldSchema, f.doc(), f.defaultVal());
                copyProps(f, nf);
                fa = fa.name(nf.name()).type(nf.schema()).withDefault(nf.defaultVal());
            } else {
                Schema.Field nf = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
                copyProps(f, nf);
                fa = fa.name(nf.name()).type(nf.schema()).withDefault(nf.defaultVal());
            }
        }

        Schema rebuilt = fa.endRecord();
        copyRecordProps(envelope, rebuilt);
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
        System.out.println(">>> [KcopHandler] Extracting table record schema from envelope: " + envelopeSchema.getFullName());
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

}
