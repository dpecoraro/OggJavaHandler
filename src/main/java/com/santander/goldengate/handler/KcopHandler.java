package com.santander.goldengate.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.directory.NoSuchAttributeException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.santander.goldengate.helpers.EntityTypeFormatHandler;
import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.AbstractHandler;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsEvent;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsRecord;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

/**
 * Handler para processar operações do GoldenGate (INSERT, UPDATE, DELETE)
 */
public class KcopHandler extends AbstractHandler {

    private int operationCount = 0;
    private String kafkaProducerConfigFile;
    private DsMetaData metaData;
    private AvroSchemaManager schemaManager;
    private SchemaTypeConverter schemaTypeConverter;
    private KafkaProducer<Object, GenericRecord> kafkaProducer; // key via Avro serializer to auto-register in SR
    private String topicMappingTemplate;
    private String kafkaBootstrapServers;
    private SchemaRegistryClient schemaRegistryClient;

    private String lastRegisteredTopic = null;
    private Map<String, String[]> keyColumnsOverrides = new HashMap<>();
    private Map<String, LinkedHashMap<String, Integer>> defaultKeyColumnSpecs = new HashMap<>();

    private final java.util.Set<String> loggedLenCols = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    public KcopHandler() {
        System.out.println(">>> [KcopHandler] Constructor called");
        LinkedHashMap<String, Integer> aedt074 = new LinkedHashMap<>();
        aedt074.put("CD_BANC", 4);
        aedt074.put("CD_CENT_CPTU", 4);
        aedt074.put("AN_PROP", 4);
        aedt074.put("NR_SOLI", 8);
        defaultKeyColumnSpecs.put("AEDT074", aedt074);
    }

    public void setKafkaProducerConfigFile(String kafkaProducerConfigFile) {
        this.kafkaProducerConfigFile = kafkaProducerConfigFile;
        System.out.println(">>> [KcopHandler] kafkaProducerConfigFile set to " + kafkaProducerConfigFile);
    }

    @Override
    public void init(DsConfiguration config, DsMetaData metaData) {
        System.out.println(">>> [KcopHandler] init() called");
        super.init(config, metaData);
        this.metaData = metaData;

        // Initialize Kafka Producer
        try {
            Properties kafkaProps = new Properties();
            if (kafkaProducerConfigFile != null) {
                try (FileInputStream fis = new FileInputStream(kafkaProducerConfigFile)) {
                    kafkaProps.load(fis);
                    System.out.println(">>> [KcopHandler] Loaded Kafka producer properties from " + kafkaProducerConfigFile);
                }
            } else {
                throw new NoSuchAttributeException("lack of kafka producer config file");
            }
            // Read topic template and bootstrap
            this.topicMappingTemplate = kafkaProps.getProperty("gg.handler.kafkahandler.topicMappingTemplate");
            this.kafkaBootstrapServers = kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            // Namespace prefix and schema manager
            String namespacePrefix = kafkaProps.getProperty("gg.handler.kafkahandler.namespacePrefix", "value.SOURCEDB.BALP");
            this.schemaTypeConverter = new SchemaTypeConverter();
            this.schemaManager = new AvroSchemaManager(namespacePrefix, schemaTypeConverter);

            // init registry client (optional, KafkaAvroSerializer will register automatically)
            schemaRegistryClient = new SchemaRegistryClient();
            schemaRegistryClient.init(kafkaProps);

            // Ensure schema.registry.url is set for KafkaAvroSerializer
            if (kafkaProps.getProperty("schema.registry.url") == null || kafkaProps.getProperty("schema.registry.url").isEmpty()) {
                String valueUrls = kafkaProps.getProperty("value.converter.schema.registry.url");
                String keyUrls = kafkaProps.getProperty("key.converter.schema.registry.url");
                String raw = (valueUrls != null && !valueUrls.isEmpty()) ? valueUrls : keyUrls;
                if (raw != null && !raw.isEmpty()) {
                    kafkaProps.put("schema.registry.url", raw);
                }
            }

            // Use Avro serializers for both key and value
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");

            // Parse key columns overrides: gg.handler.kafkahandler.keyColumns.<TABLE>=COL1,COL2,...
            for (String propName : kafkaProps.stringPropertyNames()) {
                if (propName.startsWith("gg.handler.kafkahandler.keyColumns.")) {
                    String tableCode = propName.substring(propName.lastIndexOf('.') + 1).toUpperCase();
                    String raw = kafkaProps.getProperty(propName, "");
                    String[] cols = Arrays.stream(raw.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .toArray(String[]::new);
                    if (cols.length > 0) {
                        keyColumnsOverrides.put(tableCode, cols);
                        System.out.println(">>> [KcopHandler] Key columns override loaded for " + tableCode + ": " + Arrays.toString(cols));
                    }
                }
            }

            kafkaProducer = new KafkaProducer<>(kafkaProps);
            System.out.println(">>> [KcopHandler] Kafka Producer initialized");
            System.out.println(">>> [KcopHandler] Kafka bootstrap.servers: " + kafkaBootstrapServers);
            System.out.println(">>> [KcopHandler] Namespace prefix: " + namespacePrefix);
            if (topicMappingTemplate != null) {
                System.out.println(">>> [KcopHandler] Topic template: " + topicMappingTemplate);
            }
        } catch (IOException | NoSuchAttributeException ex) {
            System.err.println("[KcopHandler] Error initializing Kafka Producer: " + ex.getMessage());
        }
    }

    @Override
    public Status operationAdded(DsEvent event, DsTransaction tx, DsOperation operation) {
        try {
            if (operation == null) {
                return Status.OK;
            }

            operationCount++;
            if (operationCount % 100 == 0) {
                System.out.println(">>> [KcopHandler] Processed: " + operationCount);
            }

            // pass event to processOperation
            processOperation(event, tx, operation);
            return Status.OK;

        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error in operationAdded: " + ex.getMessage());
            return Status.CHKPT;
        }
    }

    // include event to read operation timestamp
    private void processOperation(DsEvent event, DsTransaction tx, DsOperation operation) {
        if (tx == null || operation == null) {
            System.out.println(">>> [KcopHandler] Warning: tx/operation null");
            return;
        }

        final String table = operation.getTableName() != null ? operation.getTableName().toString() : "UNKNOWN";

        EntityTypeFormatHandler enttypHandler = new EntityTypeFormatHandler();
        final String opType = enttypHandler.mapEntTyp(operation);

        TableMetaData tableMetaData = (metaData != null && operation.getTableName() != null)
                ? metaData.getTableMetaData(operation.getTableName())
                : null;

        Map<String, Object> beforeImage = new LinkedHashMap<>();
        Map<String, Object> afterImage = new LinkedHashMap<>();

        DsRecord record = operation.getRecord();
        if (record != null && record.getColumns() != null) {
            int idx = 0;
            for (DsColumn c : record.getColumns()) {
                String columnName = getColumnNameByIndex(idx, tableMetaData);
                Object afterVal = c.hasAfterValue() ? c.getAfterValue() : null;
                if (afterVal != null) {
                    afterImage.put(columnName, extractValue(afterVal));
                }
                Object beforeVal = c.hasBeforeValue() ? c.getBeforeValue() : null;
                if (beforeVal != null) {
                    beforeImage.put(columnName, extractValue(beforeVal));
                }
                idx++;
            }
        } else {
            System.out.println(">>> [KcopHandler] Warning: record/columns null for table " + table);
        }

        try {
            Schema avroSchema = schemaManager.getOrCreateAvroSchema(table, tableMetaData);
            Schema avroSchemaFixed = rebuildEnvelopeWithClonedTableSchema(avroSchema, tableMetaData);
            System.out.println(">>> [KcopHandler] Using Avro schema: " + avroSchemaFixed.getFields());

            GenericRecord cdcRecord = new GenericData.Record(avroSchemaFixed);
            System.out.println(">>> [KcopHandler] Created CDC GenericRecord " + cdcRecord.getSchema());
            if (!beforeImage.isEmpty()) {
                GenericRecord beforeRec = createTableRecord(avroSchemaFixed, "beforeImage", beforeImage);
                cdcRecord.put("beforeImage", beforeRec);
            } else {
                cdcRecord.put("beforeImage", null);
            }
            if (!afterImage.isEmpty()) {
                GenericRecord afterRec = createTableRecord(avroSchemaFixed, "afterImage", afterImage);
                cdcRecord.put("afterImage", afterRec);
            } else {
                cdcRecord.put("afterImage", null);
            }

            cdcRecord.put("A_ENTTYP", opType);
            cdcRecord.put("A_CCID", tx.getTranID() != null ? tx.getTranID().toString() : null);
            // Use event/operation timestamp with space separator and 12 fractional digits
            cdcRecord.put("A_TIMSTAMP", formatMillisSpace12(extractOperationTimestampMillis(event, tx, operation))); // changed
            String ggUser = extractUser(event, tx, operation);
            // Fallback to system user if missing
            String sysUser = System.getProperty("user.name", "unknown");
            cdcRecord.put("A_JOBUSER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser); // changed
            cdcRecord.put("A_USER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser);    // changed

            // Build topic
            final String topic = resolveTopic(topicMappingTemplate, table);

            // Build Avro key schema (RECORD) and key GenericRecord from PK columns
            Schema keySchema = buildRecordKeySchema(table, tableMetaData);
            GenericRecord keyRecord = buildRecordKey(keySchema, tableMetaData, beforeImage, afterImage);

            // Log control fields and key
            System.out.println(">>> [KcopHandler] Prepared message:"
                    + " topic=" + topic
                    + " keyRecord=" + keyRecord
                    + " keySchema=" + keySchema.getFullName()
                    + " A_ENTTYP=" + cdcRecord.get("A_ENTTYP")
                    + " A_CCID=" + cdcRecord.get("A_CCID")
                    + " A_TIMSTAMP=" + cdcRecord.get("A_TIMSTAMP")
                    + " A_JOBUSER=" + cdcRecord.get("A_JOBUSER")
                    + " A_USER=" + cdcRecord.get("A_USER"));

            // Register schemas once per topic (value and key) — RECORD key
            if (lastRegisteredTopic == null || !lastRegisteredTopic.equals(topic)) {
                String valueSubject = topic + "_v2-value";
                String keySubject = topic + "_v2-key";

                System.out.println(">>> [KcopHandler] Registering value schema:"
                        + " subject=" + valueSubject
                        + " schemaName=" + avroSchemaFixed.getFullName());
                schemaRegistryClient.registerIfNeeded(valueSubject, avroSchemaFixed);

                System.out.println(">>> [KcopHandler] Registering key schema:"
                        + " subject=" + keySubject
                        + " schema=" + keySchema.toString());
                schemaRegistryClient.registerIfNeeded(keySubject, keySchema);

                System.out.println(">>> [KcopHandler] Schema registry subjects registered:"
                        + " valueSubject=" + valueSubject
                        + " keySubject=" + keySubject);
                lastRegisteredTopic = topic;
            }

            System.out.println(">>> [KcopHandler] Envelope schema (pretty): " + avroSchemaFixed.toString(true));
            System.out.println(">>> [KcopHandler] CDC Record payload: " + cdcRecord);
            System.out.println(">>> [KcopHandler] Key Record payload: " + keyRecord);
            System.out.println(">>> [KcopHandler] BeforeImage map: " + beforeImage);
            System.out.println(">>> [KcopHandler] AfterImage map: " + afterImage);

            // Send with Avro-serialized key (GenericRecord) and Avro-serialized value
            ProducerRecord<Object, GenericRecord> producerRecord = new ProducerRecord<>(topic, keyRecord, cdcRecord);
            System.out.println(">>> [KcopHandler] Sending to Kafka: bootstrap=" + kafkaBootstrapServers
                    + " topic=" + topic
                    + " key.schema=" + keySchema.getFullName());

            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[KcopHandler] Kafka send error: " + exception.getMessage());
                } else {
                    System.out.println(">>> [KcopHandler] Sent OK: topic=" + metadata.topic()
                            + " partition=" + metadata.partition()
                            + " offset=" + metadata.offset()
                            + " timestamp=" + metadata.timestamp());
                }
            });

            System.out.println(">>> SCHEMA: " + avroSchemaFixed.toString(true));
            System.out.println(">>> CDC Record: " + cdcRecord);
        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error creating/sending Avro: " + ex.getMessage());
        }
    }

    // Extract inner table record schema from the envelope (tries beforeImage, then afterImage)
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

    private Schema cloneRecordWithCharLengths(Schema record, TableMetaData tmd) {
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

                // deixa explícito: BYTE length
                s2.addProp("length", String.valueOf(byteLen));

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

    private void copySchemaPropsExcept(Schema from, Schema to, String... excluded) {
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

    private Schema rebuildEnvelopeWithClonedTableSchema(Schema envelope, TableMetaData tmd) {
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

    // Build RECORD key schema based on PK columns (or overrides or defaults)
    private Schema buildRecordKeySchema(String table, TableMetaData tableMetaData) {
        String shortName = table != null && table.contains(".")
                ? table.substring(table.lastIndexOf('.') + 1)
                : table;
        String recordNameLower = shortName != null ? shortName.toLowerCase() : "table"; // force lower-case
        String tableUpper = shortName != null ? shortName.toUpperCase() : "TABLE";

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                .record(recordNameLower) // lower-case to match SR subjects
                .namespace("key.SOURCEDB.BALP")
                .fields();

        // 1) Property override takes precedence
        String[] overrideCols = keyColumnsOverrides.get(tableUpper);
        if (overrideCols != null && overrideCols.length > 0) {
            System.out.println(">>> [KcopHandler] Using key columns override for " + tableUpper + ": " + Arrays.toString(overrideCols));
            for (String colName : overrideCols) {
                ColumnMetaData col = findColumnByName(tableMetaData, colName);
                Schema colSchema = Schema.create(Type.STRING);
                // Heuristic: assign TIMESTAMP/DATE if name suggests
                if (colName.toUpperCase().startsWith("DH_") || "DH_TRMT".equalsIgnoreCase(colName)) {
                    colSchema.addProp("logicalType", "TIMESTAMP");
                    colSchema.addProp("length", 32);
                } else if (colName.toUpperCase().startsWith("DT_")) {
                    colSchema.addProp("logicalType", "DATE");
                    colSchema.addProp("length", 10);
                } else {
                    colSchema.addProp("logicalType", "CHARACTER");
                    colSchema.addProp("length", col != null ? safeGetCharLength(col) : 255);
                }
                colSchema.addProp("dbColumnName", col != null ? col.getColumnName() : colName);
                fields.name(colName).type(colSchema).withDefault("");
            }
            return fields.endRecord();
        }

        // 2) Default spec per table (fixed lengths)
        LinkedHashMap<String, Integer> defaults = defaultKeyColumnSpecs.get(tableUpper);
        if (defaults != null && !defaults.isEmpty()) {
            System.out.println(">>> [KcopHandler] Using default key spec for " + tableUpper + ": " + defaults.keySet());
            for (Map.Entry<String, Integer> e : defaults.entrySet()) {
                String colName = e.getKey();
                int len = e.getValue() != null ? e.getValue() : 255;
                ColumnMetaData col = findColumnByName(tableMetaData, colName);
                Schema colSchema = Schema.create(Type.STRING);
                // Heuristic: assign TIMESTAMP/DATE if name suggests
                if (colName.toUpperCase().startsWith("DH_") || "DH_TRMT".equalsIgnoreCase(colName)) {
                    colSchema.addProp("logicalType", "TIMESTAMP");
                    colSchema.addProp("length", 32);
                } else if (colName.toUpperCase().startsWith("DT_")) {
                    colSchema.addProp("logicalType", "DATE");
                    colSchema.addProp("length", 10);
                } else {
                    colSchema.addProp("logicalType", "CHARACTER");
                    colSchema.addProp("length", len);
                }
                colSchema.addProp("dbColumnName", col != null ? col.getColumnName() : colName);
                fields.name(colName).type(colSchema).withDefault("");
            }
            return fields.endRecord();
        }

        // 3) Fallback to GG metadata isKeyCol() with type inference
        if (tableMetaData != null) {
            LinkedHashMap<String, Schema> selected = new LinkedHashMap<>();
            for (int i = 0; i < tableMetaData.getNumColumns(); i++) {
                ColumnMetaData col = tableMetaData.getColumnMetaData(i);
                if (col == null || !col.isKeyCol()) {
                    continue;
                }

                String colName = col.getColumnName();
                String typeName = col.getDataType() != null ? col.getDataType().toString().toUpperCase() : "";
                boolean isDecimalLike = typeName.contains("NUMBER") || typeName.contains("DECIMAL") || typeName.contains("NUMERIC");
                int scale = getNumericScale(col);

                Schema colSchema;
                // Prefer name-based heuristic first to match DH_/DT_ keys
                if (colName.toUpperCase().startsWith("DH_") || "DH_TRMT".equalsIgnoreCase(colName)) {
                    colSchema = Schema.create(Type.STRING);
                    colSchema.addProp("logicalType", "TIMESTAMP");
                    colSchema.addProp("length", 32);
                } else if (colName.toUpperCase().startsWith("DT_")) {
                    colSchema = Schema.create(Type.STRING);
                    colSchema.addProp("logicalType", "DATE");
                    colSchema.addProp("length", 10);
                } else if (typeName.contains("TIMESTAMP") || typeName.contains("TIME")) {
                    colSchema = Schema.create(Type.STRING);
                    colSchema.addProp("logicalType", "TIMESTAMP");
                    colSchema.addProp("length", 32);
                } else if (typeName.contains("DATE")) {
                    colSchema = Schema.create(Type.STRING);
                    colSchema.addProp("logicalType", "DATE");
                    colSchema.addProp("length", 10);
                } else if (isDecimalLike && scale == 0) {
                    colSchema = Schema.create(Type.LONG);
                    colSchema.addProp("logicalType", "DECIMAL");
                    colSchema.addProp("precision", getNumericPrecision(col));
                    colSchema.addProp("scale", 0);
                } else {
                    colSchema = Schema.create(Type.STRING);
                    colSchema.addProp("logicalType", "CHARACTER");
                    colSchema.addProp("length", safeGetCharLength(col));
                }
                colSchema.addProp("dbColumnName", colName);
                selected.put(colName, colSchema);
            }
            if (!selected.isEmpty()) {
                System.out.println(">>> [KcopHandler] Using GG key columns for " + tableUpper + ": " + selected.keySet());
                for (Map.Entry<String, Schema> e : selected.entrySet()) {
                    fields.name(e.getKey()).type(e.getValue()).withDefault(schemaTypeConverter.getDefaultValue(e.getValue()));
                }
            }
        }
        return fields.endRecord();
    }

    // Find a column by name (case-insensitive)
    private ColumnMetaData findColumnByName(TableMetaData tmd, String name) {
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

    private int safeGetCharLength(ColumnMetaData col) {
        if (col == null) {
            return 255;
        }

        String colName = null;
        try {
            colName = col.getColumnName();
        } catch (Exception ignore) {
        }
        String key = (colName == null ? "UNKNOWN" : colName);

        // loga 1x por coluna
        if (loggedLenCols.add(key)) {
            System.out.println(">>> [LEN-DEBUG] column=" + key
                    + " class=" + col.getClass().getName()
                    + " dataType=" + (col.getDataType() != null ? col.getDataType().toString() : "null"));

            String[] probes = new String[]{
                "getLength", "getCharLength", "getColumnLength", "getDataLength",
                "getBinaryLength", "getDisplaySize", "getPrecision", "getScale"
            };

            for (String mName : probes) {
                try {
                    Method m = col.getClass().getMethod(mName);
                    Object v = m.invoke(col);
                    System.out.println(">>> [LEN-DEBUG] " + key + "." + mName + "=" + v);
                } catch (Exception e) {
                    System.out.println(">>> [LEN-DEBUG] " + key + "." + mName + " (n/a)");
                }
            }
        }

        // tentativa “boa” de char-length
        String[] candidates = new String[]{"getCharLength", "getColumnLength", "getLength"};
        for (String mName : candidates) {
            try {
                Method m = col.getClass().getMethod(mName);
                Object v = m.invoke(col);
                if (v instanceof Number) {
                    int len = ((Number) v).intValue();
                    if (len > 0) {
                        return len;
                    }
                }
            } catch (Exception ignore) {
            }
        }

        // fallback: parse do dataType (se vier algo tipo VARCHAR2(4 CHAR))
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('('), r = dt.indexOf(')');
                if (l > 0 && r > l) {
                    String inside = dt.substring(l + 1, r).trim();
                    if (inside.contains(",")) {
                        inside = inside.substring(0, inside.indexOf(','));
                    }
                    int parsed = Integer.parseInt(inside.replaceAll("[^0-9]", ""));
                    if (parsed > 0) {
                        return parsed;
                    }
                }
            }
        } catch (Exception ignore) {
        }

        return 255;
    }

    // Build GenericRecord key from afterImage/beforeImage map
    private GenericRecord buildRecordKey(Schema keySchema,
            TableMetaData tableMetaData,
            Map<String, Object> beforeImage,
            Map<String, Object> afterImage) {
        GenericRecord keyRecord = new GenericData.Record(keySchema);
        for (org.apache.avro.Schema.Field f : keySchema.getFields()) {
            String name = f.name();
            Object raw = afterImage.get(name);
            if (raw == null) {
                raw = beforeImage.get(name);
            }
            Object converted = convertValueToSchemaType(raw, f.schema(), name);
            keyRecord.put(name, converted);
        }
        return keyRecord;
    }

    // Create a table image record ("beforeImage" or "afterImage") using the envelope schema
    private GenericRecord createTableRecord(Schema envelopeSchema, String fieldName, Map<String, Object> image) {
        if (envelopeSchema == null || fieldName == null) {
            return null;
        }
        Field field = envelopeSchema.getField(fieldName);
        if (field == null) {
            System.err.println("[KcopHandler] Envelope schema missing field: " + fieldName);
            return null;
        }
        Schema fieldSchema = field.schema();
        // If field is a union, pick the non-null record schema
        if (fieldSchema.getType() == Type.UNION) {
            for (Schema s : fieldSchema.getTypes()) {
                if (s.getType() == Type.RECORD) {
                    fieldSchema = s;
                    break;
                }
            }
        }
        if (fieldSchema.getType() != Type.RECORD) {
            System.err.println("[KcopHandler] Field " + fieldName + " is not a RECORD schema");
            return null;
        }

        GenericRecord rec = new GenericData.Record(fieldSchema);
        for (Schema.Field colField : fieldSchema.getFields()) {
            String colName = colField.name();
            Object raw = image != null ? image.get(colName) : null;
            Object converted = convertValueToSchemaType(raw, colField.schema(), colName);
            rec.put(colName, converted);
        }
        return rec;
    }

    // Overload: enforce yyyy-MM-dd for DATE and handle DECIMAL according to schema type (STRING vs numeric)
    protected Object convertValueToSchemaType(Object value, Schema schema, String fieldName) {
        if (value == null) {
            return schemaTypeConverter.getDefaultValue(schema);
        }
        Object out = convertValueToSchemaType(value, schema); // base logic

        try {
            String logical = schema.getProp("logicalType");
            Type schemaType = schema.getType();

            // DECIMAL: if schema type is STRING, format as "0.00"; if numeric, return numeric respecting scale (scale=0 -> integer)
            boolean isDecimalLogical = logical != null && "DECIMAL".equalsIgnoreCase(logical);
            boolean isDecimalFieldName = "VL_ALCA_PROP".equalsIgnoreCase(fieldName); // explicit per feedback
            if (isDecimalLogical || isDecimalFieldName) {
                int scale = 0;
                try {
                    String prop = schema.getProp("scale");
                    if (prop != null && !prop.isEmpty()) {
                        scale = Integer.parseInt(prop);
                    }
                } catch (NumberFormatException ignore) {
                }

                String rawStr = (value == null) ? null : value.toString();

                if (schemaType == Type.STRING) {
                    int outScale = Math.max(2, scale); // garante 2 casas mesmo se scale=0
                    String formatted = formatDecimalString(rawStr, outScale);
                    return formatted;
                } else {
                    // Numeric schema: parse and return numeric (no string) to avoid Avro type mismatch
                    try {
                        String norm = rawStr == null ? "0" : rawStr.trim().replace(',', '.');
                        BigDecimal bd = new BigDecimal(norm);
                        bd = bd.setScale(Math.max(0, scale), RoundingMode.HALF_UP);
                        switch (schemaType) {
                            case LONG:
                                return bd.longValue();
                            case INT:
                                return bd.intValue();
                            case DOUBLE:
                                return bd.doubleValue();
                            case FLOAT:
                                return bd.floatValue();
                            default:
                                return out; // fallback to base
                        }
                    } catch (Exception e) {
                        // fallback: base conversion already handled numerics; return default if needed
                        return out instanceof Number ? out : schemaTypeConverter.getDefaultValue(schema);
                    }
                }
            }

            // DATE trimming (yyyy-MM-dd)
            boolean isDateLogical = logical != null && "DATE".equalsIgnoreCase(logical);
            boolean isDateFieldName = fieldName != null && fieldName.toUpperCase().startsWith("DT_");
            if ((isDateLogical || isDateFieldName) && out instanceof CharSequence) {
                String s = out.toString().replace('/', '-');
                int cutIdx = -1;
                int spaceIdx = s.indexOf(' ');
                int tIdx = s.indexOf('T');
                if (spaceIdx > 0) {
                    cutIdx = spaceIdx;
                } else if (tIdx > 0) {
                    cutIdx = tIdx;
                }
                String dateOnly = cutIdx > 0 ? s.substring(0, cutIdx) : s;
                if (dateOnly.matches("\\d{8}")) {
                    return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
                }
                return dateOnly.length() >= 10 ? dateOnly.substring(0, 10) : dateOnly;
            }
        } catch (Exception ignore) {
        }
        return out;
    }

    protected Object convertValueToSchemaType(Object value, Schema schema) {
        if (value == null) {
            return schemaTypeConverter.getDefaultValue(schema);
        }
        Type type = schema.getType();
        try {
            switch (type) {
                case INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.valueOf(value.toString().trim());
                case LONG:
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    String ls = value.toString().trim();
                    if (ls.contains(".")) {
                        double d = Double.parseDouble(ls);
                        return (long) Math.round(d);
                    }
                    return Long.valueOf(ls);
                case FLOAT:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    }
                    return Float.valueOf(value.toString().trim());
                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.valueOf(value.toString().trim());
                case STRING: {
                    String s = value.toString();
                    String logical = schema.getProp("logicalType");

                    // DECIMAL: format as string with fixed scale (e.g., "0.00")
                    if (logical != null && "DECIMAL".equalsIgnoreCase(logical)) {
                        int scale = 2;
                        try {
                            String prop = schema.getProp("scale");
                            if (prop != null && !prop.isEmpty()) {
                                scale = Integer.parseInt(prop);
                            }
                        } catch (NumberFormatException ignore) {
                        }
                        return formatDecimalString(s, scale);
                    }

                    // DATE: strictly yyyy-MM-dd (remove 'T' and any time/fraction)
                    if (logical != null && "DATE".equalsIgnoreCase(logical)) {
                        String norm = s.replace('/', '-');
                        int cutIdx = -1;
                        int spaceIdx = norm.indexOf(' ');
                        int tIdx = norm.indexOf('T');
                        if (spaceIdx > 0) {
                            cutIdx = spaceIdx;
                        } else if (tIdx > 0) {
                            cutIdx = tIdx;
                        }
                        String dateOnly = cutIdx > 0 ? norm.substring(0, cutIdx) : norm;
                        if (dateOnly.matches("\\d{8}")) {
                            return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
                        }
                        return dateOnly.length() >= 10 ? dateOnly.substring(0, 10) : dateOnly;
                    }

                    // TIMESTAMP: ISO with 'T' and 12 fractional digits
                    if (logical != null && "TIMESTAMP".equalsIgnoreCase(logical)) {
                        String iso = s.replace(' ', 'T');
                        int dotIdx = iso.indexOf('.');
                        if (dotIdx < 0) {
                            // No fractional part: append 12 zeros
                            return iso + ".000000000000";
                        }
                        int endIdx = iso.indexOf('Z') > 0 ? iso.indexOf('Z') : iso.length();
                        String prefix = iso.substring(0, dotIdx + 1);
                        String fracAndRest = iso.substring(dotIdx + 1, endIdx);
                        StringBuilder digits = new StringBuilder();
                        for (int i = 0; i < fracAndRest.length(); i++) {
                            char c = fracAndRest.charAt(i);
                            if (Character.isDigit(c)) {
                                digits.append(c);
                            } else {
                                break;
                            }
                        }
                        String frac = digits.toString();
                        if (frac.length() > 12) {
                            frac = frac.substring(0, 12);
                        } else if (frac.length() < 12) {
                            StringBuilder pad = new StringBuilder(frac);
                            while (pad.length() < 12) {
                                pad.append('0');
                            }
                            frac = pad.toString();
                        }
                        String remainder = iso.substring(dotIdx + 1 + digits.length(), endIdx);
                        return prefix + frac + remainder + (endIdx < iso.length() ? iso.substring(endIdx) : "");
                    }

                    return s;
                }
                case BYTES:
                    if (value instanceof byte[]) {
                        return ByteBuffer.wrap((byte[]) value);
                    }
                    if (value instanceof ByteBuffer) {
                        return value;
                    }
                    try {
                        return ByteBuffer.wrap(Base64.getDecoder().decode(value.toString()));
                    } catch (IllegalArgumentException e) {
                        return ByteBuffer.wrap(value.toString().getBytes());
                    }
                default:
                    return value.toString();
            }
        } catch (NumberFormatException e) {
            System.err.println("[KcopHandler] Error converting value " + value + " to type " + type + ": " + e.getMessage());
            return schemaTypeConverter.getDefaultValue(schema);
        }
    }

    // Helper: format decimal string with fixed scale (no scientific notation)
    private String formatDecimalString(String raw, int scale) {
        try {
            if (raw == null || raw.trim().isEmpty()) {
                return zeroOfScale(scale);
            }
            String norm = raw.trim().replace(',', '.');
            BigDecimal bd = new BigDecimal(norm);
            bd = bd.setScale(scale, RoundingMode.HALF_UP);
            return bd.toPlainString();
        } catch (Exception e) {
            // Fallback: try parse as number; else return zero with scale
            try {
                BigDecimal bd = new BigDecimal(String.valueOf(Double.parseDouble(raw)));
                bd = bd.setScale(scale, RoundingMode.HALF_UP);
                return bd.toPlainString();
            } catch (Exception ignore) {
                return zeroOfScale(scale);
            }
        }
    }

    private String zeroOfScale(int scale) {
        if (scale <= 0) {
            return "0";
        }
        StringBuilder sb = new StringBuilder("0.");
        for (int i = 0; i < scale; i++) {
            sb.append('0');
        }
        return sb.toString();
    }

    private String formatMillisSpace12(long millis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        String base = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        // nanos: 0..999_999_999 (9 digits), pad right to 12 digits
        String frac9 = String.format("%09d", ldt.getNano());
        StringBuilder frac12 = new StringBuilder(frac9);
        while (frac12.length() < 12) {
            frac12.append('0');
        }
        if (frac12.length() > 12) {
            frac12.setLength(12);
        }
        return base + "." + frac12;
    }

    private String getColumnNameByIndex(int index, TableMetaData tableMetaData) {
        try {
            ColumnMetaData colMeta = safeGetColumnMetaData(tableMetaData, index);
            if (colMeta != null) {
                return colMeta.getColumnName();
            }
        } catch (Exception e) {
            System.err.println("[KcopHandler] Error getting column name at index " + index + ": " + e.getMessage());
        }
        return "COL_" + index;
    }

    protected Object extractValue(Object value) {
        try {
            if (value == null) {
                return null;
            }
            if (value instanceof byte[]) {
                return Base64.getEncoder().encodeToString((byte[]) value);
            }
            return value;
        } catch (Exception ignore) {
            return null;
        }
    }

    @Override
    public Status transactionCommit(DsEvent event, DsTransaction tx) {
        return Status.OK;
    }

    @Override
    public void destroy() {
        System.out.println(">>> [KcopHandler] destroy() called");
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
            System.out.println(">>> [KcopHandler] Kafka Producer closed");
        }
    }

    @Override
    public String reportStatus() {
        return "[KcopHandler] OK (Processed: " + operationCount + ")";
    }

    // Safe access to metadata column by index, returns null when out-of-range or on error
    private ColumnMetaData safeGetColumnMetaData(TableMetaData tableMetaData, int index) {
        if (tableMetaData == null || index < 0) {
            return null;
        }
        try {
            return tableMetaData.getColumnMetaData(index);
        } catch (IndexOutOfBoundsException ex) {
            return null;
        }
    }

    // Resolve topic from template; fallback keeps previous behavior if template is missing
    protected String resolveTopic(String template, String fullyQualifiedTableName) {
        if (template == null || template.isEmpty()) {
            return "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_");
        }
        return template.replace("${fullyQualifiedTableName}", fullyQualifiedTableName);
    }

    // Safely obtain numeric scale from ColumnMetaData using reflection; returns -1 if unavailable
    private int getNumericScale(ColumnMetaData col) {
        if (col == null) {
            return -1;
        }
        String[] candidates = new String[]{
            "getScale",
            "getColumnScale",
            "getFractionalDigits",
            "getDecimalDigits"
        };
        for (String mName : candidates) {
            try {
                Method m = col.getClass().getMethod(mName);
                Object v = m.invoke(col);
                if (v instanceof Number) {
                    return ((Number) v).intValue();
                }
            } catch (Exception ignore) {
                // try next candidate
            }
        }
        // Some implementations encode scale in data type string like "NUMBER(p,s)"
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('(');
                int r = dt.indexOf(')');
                if (l >= 0 && r > l) {
                    String inside = dt.substring(l + 1, r);
                    String[] parts = inside.split(",");
                    if (parts.length == 2) {
                        return Integer.parseInt(parts[1].trim());
                    }
                }
            }
        } catch (Exception ignore) {
        }
        return -1; // unknown scale
    }

    // Safely obtain numeric precision from ColumnMetaData using reflection; returns default 38 if unavailable
    private int getNumericPrecision(ColumnMetaData col) {
        if (col == null) {
            return 38;
        }
        String[] candidates = new String[]{
            "getPrecision",
            "getColumnPrecision",
            "getLength",
            "getDisplaySize"
        };
        for (String mName : candidates) {
            try {
                Method m = col.getClass().getMethod(mName);
                Object v = m.invoke(col);
                if (v instanceof Number) {
                    return Math.max(1, ((Number) v).intValue());
                }
            } catch (Exception ignore) {
                // try next candidate
            }
        }
        // Parse precision from data type string, e.g., "NUMBER(p,s)" or "DECIMAL(p,s)"
        try {
            String dt = col.getDataType() != null ? col.getDataType().toString() : null;
            if (dt != null) {
                int l = dt.indexOf('(');
                int r = dt.indexOf(')');
                if (l >= 0 && r > l) {
                    String inside = dt.substring(l + 1, r);
                    String[] parts = inside.split(",");
                    if (parts.length >= 1) {
                        return Math.max(1, Integer.parseInt(parts[0].trim()));
                    }
                }
            }
        } catch (Exception ignore) {
        }
        // Reasonable default precision for NUMBER/DECIMAL when not provided
        return 38;
    }

    // Try to get operation/event timestamp in millis; fallback to System.currentTimeMillis()
    private long extractOperationTimestampMillis(DsEvent event, DsTransaction tx, DsOperation operation) {
        Long fromEvent = tryGetMillisViaReflection(event, "getTimestamp");
        if (fromEvent != null) {
            return fromEvent;
        }

        // Try: operation.getTimestamp()
        Long fromOp = tryGetMillisViaReflection(operation, "getTimestamp");
        if (fromOp != null) {
            return fromOp;
        }

        // Try: tx.getTimestamp()
        Long fromTx = tryGetMillisViaReflection(tx, "getTimestamp");
        if (fromTx != null) {
            return fromTx;
        }

        // Fallback
        return System.currentTimeMillis();
    }

    // Helper: call obj.methodName() and convert to millis if it returns Date/Long/String
    private Long tryGetMillisViaReflection(Object obj, String methodName) {
        if (obj == null) {
            return null;
        }
        try {
            Method m = obj.getClass().getMethod(methodName);
            Object val = m.invoke(obj);
            if (val == null) {
                return null;
            }

            if (val instanceof Date) {
                return ((Date) val).getTime();
            }
            if (val instanceof Number) {
                return ((Number) val).longValue();
            }
            if (val instanceof CharSequence) {
                // Try parse epoch millis from string; otherwise return null
                try {
                    return Long.valueOf(val.toString().trim());
                } catch (NumberFormatException ignore) {
                    return null;
                }
            }
        } catch (IllegalAccessException
                | IllegalArgumentException
                | NoSuchMethodException
                | SecurityException
                | InvocationTargetException ignore) {
            return null;
        }
        return null;
    }

    // Try to get user name from event/tx/operation via common GG methods; fallback null
    private String extractUser(DsEvent event, DsTransaction tx, DsOperation operation) {
        String user;
        // Common method names across GG APIs
        String[] methodCandidates = new String[]{
            "getUserName", "getUsername", "getUser", "getJobUser", "getOwner"
        };
        user = tryGetStringViaReflection(tx, methodCandidates);
        if (user != null && !user.isEmpty()) {
            return user;
        }
        user = tryGetStringViaReflection(operation, methodCandidates);
        if (user != null && !user.isEmpty()) {
            return user;
        }
        user = tryGetStringViaReflection(event, methodCandidates);
        return (user != null && !user.isEmpty()) ? user : null;
    }

    // Helper: call the first available method that returns a String
    private String tryGetStringViaReflection(Object obj, String[] methodNames) {
        if (obj == null || methodNames == null) {
            return null;
        }
        for (String mName : methodNames) {
            try {
                Method m = obj.getClass().getMethod(mName);
                Object val = m.invoke(obj);
                if (val instanceof CharSequence) {
                    String s = val.toString().trim();
                    if (!s.isEmpty()) {
                        return s;
                    }
                }
            } catch (Exception ignore) {
            }
        }
        return null;
    }
}
