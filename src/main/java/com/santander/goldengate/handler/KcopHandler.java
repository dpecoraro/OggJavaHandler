package com.santander.goldengate.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.santander.goldengate.helpers.CharFormatHandler;
import com.santander.goldengate.helpers.DateFormatHandler;
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
    private KafkaProducer<String, GenericRecord> kafkaProducer; // key via Avro serializer to auto-register in SR
    private String topicMappingTemplate;
    private String kafkaBootstrapServers;
    private String namespacePrefix;

    private SchemaRegistryClient schemaRegistryClient;
    private DateFormatHandler dateFormatHandler = new DateFormatHandler();

    private String lastRegisteredTopic = null;
    private Map<String, String[]> keyColumnsOverrides = new HashMap<>();
    private CharFormatHandler charFormatHandler;
    private Map<String, LinkedHashMap<String, Integer>> defaultKeyColumnSpecs = new HashMap<>();

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

    public void setTopicMappingTemplate(String topicMappingTemplate) {
        this.topicMappingTemplate = topicMappingTemplate;
        System.out.println(">>> [KcopHandler] topicMappingTemplate set to " + topicMappingTemplate);
    }

    public void setNameSpacePrefix(String namespacePrefix) {
        this.namespacePrefix = namespacePrefix;
        System.out.println(">>> [KcopHandler] namespacePrefix set to " + namespacePrefix);
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
            //this.topicMappingTemplate = kafkaProps.getProperty("gg.handler.kcoph.topicMappingTemplate");
            this.kafkaBootstrapServers = kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            // Namespace prefix and schema manager
            //String namespacePrefix = kafkaProps.getProperty("gg.handler.kcoph.namespacePrefix", "value.SOURCEDB.BALP");
            
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
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");

            for (String propName : kafkaProps.stringPropertyNames()) {
                if (propName.startsWith("gg.handler.kcoph.keyColumns.")) {
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
            Schema avroSchemaFixed = schemaTypeConverter.rebuildEnvelopeWithClonedTableSchema(avroSchema, tableMetaData);
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
            cdcRecord.put("A_TIMSTAMP", dateFormatHandler.formatMillisSpace12(extractOperationTimestampMillis(event, tx, operation))); // changed
            //String ggUser = extractUser(event, tx, operation);
            // Fallback to system user if missing
            //String sysUser = System.getProperty("user.name", "unknown");
            //cdcRecord.put("A_JOBUSER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser); // changed
            //cdcRecord.put("A_USER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser);    // changed

            // Build topic
            final String topic = resolveTopic(topicMappingTemplate, table);

            // Build Avro key schema (RECORD) and key GenericRecord from PK columns
            Schema keySchema = buildRecordKeySchema(table, tableMetaData);
            String keyRecord = buildKeyString(table, keySchema, cdcRecord);

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
                String valueSubject = topic + "-value";
                String keySubject = topic + "-key";

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

            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, keyRecord, cdcRecord);
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

    // Value conversion with logical types support (DATE/TIMESTAMP/DECIMAL)
    protected Object convertValueToSchemaType(Object value, Schema schema, String fieldName) {
        if (value == null) {
            return schemaTypeConverter.getDefaultValue(schema);
        }
        Object out = convertValueToSchemaType(value, schema);

        try {
            String logical = schema.getProp("logicalType");
            Type schemaType = schema.getType();

            // DECIMAL
            boolean isDecimalLogical = logical != null && "DECIMAL".equalsIgnoreCase(logical);
            boolean isDecimalFieldName = "VL_ALCA_PROP".equalsIgnoreCase(fieldName);
            if (isDecimalLogical || isDecimalFieldName) {
                int scale = 0;
                try {
                    String prop = schema.getProp("scale");
                    if (prop != null && !prop.isEmpty()) {
                        scale = Integer.parseInt(prop);
                    }
                } catch (NumberFormatException ignore) {
                }

                String rawStr = value.toString();
                if (schemaType == Type.STRING) {
                    int outScale = Math.max(2, scale);
                    return formatDecimalString(rawStr, outScale);
                } else {
                    try {
                        String norm = rawStr.trim().replace(',', '.');
                        BigDecimal bd = new BigDecimal(norm).setScale(Math.max(0, scale), RoundingMode.HALF_UP);
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
                                return out;
                        }
                    } catch (Exception e) {
                        return out instanceof Number ? out : schemaTypeConverter.getDefaultValue(schema);
                    }
                }
            }

            // DATE -> yyyy-MM-dd
            boolean isDateLogical = logical != null && "DATE".equalsIgnoreCase(logical);
            boolean isDateFieldName = fieldName != null && fieldName.toUpperCase().startsWith("DT_");
            if ((isDateLogical || isDateFieldName) && out instanceof CharSequence) {
                String s = out.toString().replace('/', '-');
                int cutIdx = Math.max(s.indexOf(' '), s.indexOf('T'));
                String dateOnly = cutIdx > 0 ? s.substring(0, cutIdx) : s;
                if (dateOnly.matches("\\d{8}")) {
                    return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
                }
                return dateOnly.length() >= 10 ? dateOnly.substring(0, 10) : dateOnly;
            }

            // TIMESTAMP -> ISO with 12 fractional digits and 'T'
            if (logical != null && "TIMESTAMP".equalsIgnoreCase(logical) && out instanceof CharSequence) {
                String iso = out.toString().replace(' ', 'T');
                int dotIdx = iso.indexOf('.');
                if (dotIdx < 0) {
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
                    }else {
                        break;
                    }
                }
                String frac = digits.toString();
                if (frac.length() > 12) {
                    frac = frac.substring(0, 12); 
                }else {
                    while (frac.length() < 12) {
                        frac += '0';
                    }
                }
                String remainder = iso.substring(dotIdx + 1 + digits.length(), endIdx);
                return prefix + frac + remainder + (endIdx < iso.length() ? iso.substring(endIdx) : "");
            }
        } catch (Exception ignore) {
        }

        return out;
    }

    // Base conversion by Avro primitive type
    protected Object convertValueToSchemaType(Object value, Schema schema) {
        if (value == null) {
            return schemaTypeConverter.getDefaultValue(schema);
        }
        Type type = schema.getType();
        try {
            switch (type) {
                case INT:
                    return (value instanceof Number) ? ((Number) value).intValue()
                            : Integer.valueOf(value.toString().trim());
                case LONG:
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    String ls = value.toString().trim();
                    if (ls.contains(".")) {
                        return (long) Math.round(Double.parseDouble(ls));
                    }
                    return Long.valueOf(ls);
                case FLOAT:
                    return (value instanceof Number) ? ((Number) value).floatValue()
                            : Float.valueOf(value.toString().trim());
                case DOUBLE:
                    return (value instanceof Number) ? ((Number) value).doubleValue()
                            : Double.valueOf(value.toString().trim());
                case STRING:
                    return value.toString();
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

    // Helper: format decimal string
    private String formatDecimalString(String raw, int scale) {
        try {
            if (raw == null || raw.trim().isEmpty()) {
                return zeroOfScale(scale);
            }
            String norm = raw.trim().replace(',', '.');
            BigDecimal bd = new BigDecimal(norm).setScale(scale, RoundingMode.HALF_UP);
            return bd.toPlainString();
        } catch (Exception e) {
            try {
                BigDecimal bd = new BigDecimal(String.valueOf(Double.parseDouble(raw))).setScale(scale, RoundingMode.HALF_UP);
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

    // Extract raw value (encode byte[] to Base64)
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
                ColumnMetaData col = schemaTypeConverter.findColumnByName(tableMetaData, colName);
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
                    colSchema.addProp("length", col != null ? charFormatHandler.safeGetCharLength(col) : 255);
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
                ColumnMetaData col = schemaTypeConverter.findColumnByName(tableMetaData, colName);
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
                    colSchema.addProp("length", charFormatHandler.safeGetCharLength(col));
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


    // Build GenericRecord key from afterImage/beforeImage record inside the envelope
    private String buildKeyString(String table, Schema keySchema, GenericRecord envelopeRecord) {
        // Prefer afterImage; fallback to beforeImage
        GenericRecord image = getTableImageRecord(envelopeRecord);

        StringBuilder sb = new StringBuilder();
        for (Schema.Field f : keySchema.getFields()) {
            Object v = safeGetFromRecord(image, f.name());
            String s = (v == null) ? "" : v.toString();

            int len = 0;
            try {
                String l = f.schema().getProp("length");
                if (l != null) {
                    len = Integer.parseInt(l);
                }
            } catch (Exception ignore) {
            }
            if (len > 0 && s.length() < len) {
                sb.append("0".repeat(len - s.length()));
            }
            sb.append(s);
        }
        return sb.toString();
    }

    // Helper: select the inner table image record
    private GenericRecord getTableImageRecord(GenericRecord envelopeRecord) {
        if (envelopeRecord == null) {
            return null;
        }
        Object after = null, before = null;
        try {
            after = envelopeRecord.get("afterImage");
        } catch (Exception ignore) {
        }
        try {
            before = envelopeRecord.get("beforeImage");
        } catch (Exception ignore) {
        }
        if (after instanceof GenericRecord) {
            return (GenericRecord) after;
        }
        if (before instanceof GenericRecord) {
            return (GenericRecord) before;
        }
        return null;
    }

    // Helper: safely read a field from a record (avoid AvroRuntimeException)
    private Object safeGetFromRecord(GenericRecord record, String fieldName) {
        if (record == null || fieldName == null) {
            return null;
        }
        try {
            if (record.getSchema().getField(fieldName) == null) {
                return null;
            }
            return record.get(fieldName);
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
        final String normalized = normalizeTopicTemplate(template);
        System.out.println(">>> [KcopHandler] Resolving topic for table " + fullyQualifiedTableName + " using template: " + normalized);
        if (normalized == null || normalized.isEmpty()) {
            System.out.println(">>> [KcopHandler] No topic template provided, using default topic naming: " + "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_"));
            return "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_");
        }

        String fqn = fullyQualifiedTableName != null ? fullyQualifiedTableName : "";
        String table = fqn;
        String schema = "";
        String catalog = "";

        System.out.println(">>> [KcopHandler] Parsing fully qualified table name: " + fqn);
        if (fqn.contains(".")) {
            table = fqn.substring(fqn.lastIndexOf('.') + 1);
            String prefix = fqn.substring(0, fqn.lastIndexOf('.'));
            if (prefix.contains(".")) {
                schema = prefix.substring(prefix.lastIndexOf('.') + 1);
                catalog = prefix.substring(0, prefix.lastIndexOf('.'));
            } else {
                schema = prefix;
            }
        }

        Map<String, String> vars = new HashMap<>();
        // Primary token
        vars.put("fullyQualifiedTableName", fqn);
        // Common alias users try
        vars.put("fullyQualifiedName", fqn);
        // Convenience tokens
        vars.put("table", table);
        vars.put("tableName", table);
        vars.put("schema", schema);
        vars.put("schemaName", schema);
        vars.put("catalog", catalog);
        vars.put("catalogName", catalog);

        return substitutePlaceholders(normalized, vars);
    }

    // GoldenGate/Properties may escape '$' (e.g. \${var} or \u0024{var}); normalize before substitution.
    private String normalizeTopicTemplate(String template) {
        if (template == null) {
            return null;
        }
        String t = template;
        // Unescape a leading backslash before '${'
        t = t.replace("\\${", "${");
        // If '$' was written as unicode escape sequence in the properties file
        t = t.replace("\\u0024{", "${");
        t = t.replace("\\U0024{", "${");
        return t;
    }

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    // Simple ${var} substitution; unknown vars are left untouched.
    private String substitutePlaceholders(String template, Map<String, String> vars) {
        if (template == null || template.isEmpty() || vars == null || vars.isEmpty()) {
            return template;
        }

        Matcher m = PLACEHOLDER_PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1) != null ? m.group(1).trim() : "";
            String replacement = vars.get(key);
            if (replacement == null) {
                // leave placeholder as-is
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group(0)));
            } else {
                m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        m.appendTail(sb);
        return sb.toString();
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
}
