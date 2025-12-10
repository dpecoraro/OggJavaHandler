package com.santander.goldengate.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.directory.NoSuchAttributeException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

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
    private KafkaProducer<Object, GenericRecord> kafkaProducer; // key via Avro serializer to auto-register in SR
    private String topicMappingTemplate;
    private String kafkaBootstrapServers;
    private SchemaRegistryClient schemaRegistryClient;

    private volatile boolean debugLogs = false; // simple debug flag
    private String lastRegisteredTopic = null;   // avoid repeated registry in hot loops

    public KcopHandler() {
        System.out.println(">>> [KcopHandler] Constructor called");
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

            kafkaProducer = new KafkaProducer<>(kafkaProps);
            System.out.println(">>> [KcopHandler] Kafka Producer initialized");
            System.out.println(">>> [KcopHandler] Kafka bootstrap.servers: " + kafkaBootstrapServers);
            System.out.println(">>> [KcopHandler] Namespace prefix: " + namespacePrefix);
            if (topicMappingTemplate != null) {
                System.out.println(">>> [KcopHandler] Topic template: " + topicMappingTemplate);
            }
            this.debugLogs = Boolean.parseBoolean(kafkaProps.getProperty("gg.handler.kafkahandler.debugLogs", "false"));
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
            GenericRecord cdcRecord = new GenericData.Record(avroSchema);

            if (!beforeImage.isEmpty()) {
                GenericRecord beforeRec = createTableRecord(avroSchema, "beforeImage", beforeImage);
                cdcRecord.put("beforeImage", beforeRec);
            } else {
                cdcRecord.put("beforeImage", null);
            }
            if (!afterImage.isEmpty()) {
                GenericRecord afterRec = createTableRecord(avroSchema, "afterImage", afterImage);
                cdcRecord.put("afterImage", afterRec);
            } else {
                cdcRecord.put("afterImage", null);
            }

            DateFormatHandler dateHandler = new DateFormatHandler();
            cdcRecord.put("A_ENTTYP", opType);
            cdcRecord.put("A_CCID", tx.getTranID() != null ? tx.getTranID().toString() : null);
            // Use event/operation timestamp with space separator and 12 fractional digits
            cdcRecord.put("A_TIMSTAMP", formatMillisSpace12(extractOperationTimestampMillis(event, tx, operation))); // changed
            String ggUser = extractUser(event, tx, operation);
            // Fallback to system user if missing
            String sysUser = System.getProperty("user.name", "unknown");
            cdcRecord.put("A_JOBUSER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser); // changed
            cdcRecord.put("A_USER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser);    // changed

            // Build topic and string key
            final String topic = resolveTopic(topicMappingTemplate, table);
            final String keyStr = buildKey(tx); // sanitized key

            // Log control fields and key
            System.out.println(">>> [KcopHandler] Prepared message:"
                    + " topic=" + topic
                    + " key=" + keyStr
                    + " A_ENTTYP=" + cdcRecord.get("A_ENTTYP")
                    + " A_CCID=" + cdcRecord.get("A_CCID")
                    + " A_TIMSTAMP=" + cdcRecord.get("A_TIMSTAMP")
                    + " A_JOBUSER=" + cdcRecord.get("A_JOBUSER")
                    + " A_USER=" + cdcRecord.get("A_USER"));

            // Register schemas once per topic (value and key) with STRING for key
            if (lastRegisteredTopic == null || !lastRegisteredTopic.equals(topic)) {
                String valueSubject = topic + "-value";
                String keySubject = topic + "-key";
                schemaRegistryClient.registerIfNeeded(valueSubject, avroSchema);
                schemaRegistryClient.registerIfNeeded(keySubject, Schema.create(Type.STRING)); // ensure STRING subject
                System.out.println(">>> [KcopHandler] Schema registry subjects registered:"
                        + " valueSubject=" + valueSubject
                        + " keySubject=" + keySubject);
                System.out.println(">>> [KcopHandler] Value schema: " + avroSchema.getFullName()
                        + " namespace=" + avroSchema.getNamespace()
                        + " name=" + avroSchema.getName());
                lastRegisteredTopic = topic;
            }

            // Log full Avro schema and record if debug enabled
            if (debugLogs) {
                System.out.println(">>> [KcopHandler] Envelope schema (pretty): " + avroSchema.toString(true));
                System.out.println(">>> [KcopHandler] CDC Record payload: " + cdcRecord);
                System.out.println(">>> [KcopHandler] BeforeImage map: " + beforeImage);
                System.out.println(">>> [KcopHandler] AfterImage map: " + afterImage);
            }

            // Send with Avro-serialized key as String (KafkaAvroSerializer will use Avro STRING)
            ProducerRecord<Object, GenericRecord> producerRecord = new ProducerRecord<>(topic, keyStr, cdcRecord);
            System.out.println(">>> [KcopHandler] Sending to Kafka: bootstrap=" + kafkaBootstrapServers
                    + " topic=" + topic
                    + " key=" + keyStr);

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

            if (debugLogs) {
                System.out.println(">>> SCHEMA: " + avroSchema.toString(true));
                System.out.println(">>> CDC Record: " + cdcRecord);
            }
        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error creating/sending Avro: " + ex.getMessage());
        }
    }

    // Build key from transaction (ASCII-only digits). Strips non-digits to avoid strange characters and ensures compatibility with STRING key subject.
    protected String buildKey(DsTransaction tx) {
        if (tx == null || tx.getTranID() == null) return "unknown";
        String s = tx.getTranID().toString();
        String digits = s.replaceAll("\\D+", "");
        return digits.isEmpty() ? "unknown" : digits;
    }

    private GenericRecord createTableRecord(Schema envelopeSchema, String fieldName, Map<String, Object> data) {
        Schema unionSchema = envelopeSchema.getField(fieldName).schema();
        Schema tableSchema = unionSchema.getTypes().get(1);
        GenericRecord tableRecord = new GenericData.Record(tableSchema);
        for (Field field : tableSchema.getFields()) {
            Object value = data.get(field.name());
            Object convertedValue = convertValueToSchemaType(value, field.schema());
            tableRecord.put(field.name(), convertedValue);
        }
        return tableRecord;
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

                    // DATE: strictly yyyy-MM-dd
                    if (logical != null && "DATE".equalsIgnoreCase(logical)) {
                        String norm = s.replace('/', '-');
                        int cutIdx = -1;
                        int spaceIdx = norm.indexOf(' ');
                        int tIdx = norm.indexOf('T');
                        if (spaceIdx > 0) cutIdx = spaceIdx;
                        else if (tIdx > 0) cutIdx = tIdx;
                        String dateOnly = cutIdx > 0 ? norm.substring(0, cutIdx) : norm;
                        if (dateOnly.matches("\\d{8}")) {
                            return dateOnly.substring(0, 4) + "-" + dateOnly.substring(4, 6) + "-" + dateOnly.substring(6, 8);
                        }
                        // Trim to first 10 chars if already yyyy-MM-dd...
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
                            if (Character.isDigit(c)) digits.append(c);
                            else break;
                        }
                        String frac = digits.toString();
                        if (frac.length() > 12) {
                            frac = frac.substring(0, 12);
                        } else if (frac.length() < 12) {
                            StringBuilder pad = new StringBuilder(frac);
                            while (pad.length() < 12) pad.append('0');
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

    // Format millis to "yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS" (space separator, 12 fractional digits)
    private String formatMillisSpace12(long millis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        String base = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        // nanos: 0..999_999_999 (9 digits), pad right to 12 digits
        String frac9 = String.format("%09d", ldt.getNano());
        StringBuilder frac12 = new StringBuilder(frac9);
        while (frac12.length() < 12) frac12.append('0');
        if (frac12.length() > 12) frac12.setLength(12);
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
            // fallback to previous default if no template provided
            return "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_");
        }
        return template.replace("${fullyQualifiedTableName}", fullyQualifiedTableName);
    }

    // Try to get operation/event timestamp in millis; fallback to System.currentTimeMillis()
    private long extractOperationTimestampMillis(DsEvent event, DsTransaction tx, DsOperation operation) {
        // Try: event.getTimestamp()
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
        String u;
        // Common method names across GG APIs
        String[] methodCandidates = new String[]{
            "getUserName", "getUsername", "getUser", "getJobUser", "getOwner"
        };
        // Try on tx first
        u = tryGetStringViaReflection(tx, methodCandidates);
        if (u != null && !u.isEmpty()) {
            return u;
        }
        // Then on operation
        u = tryGetStringViaReflection(operation, methodCandidates);
        if (u != null && !u.isEmpty()) {
            return u;
        }
        // Then on event
        u = tryGetStringViaReflection(event, methodCandidates);
        return (u != null && !u.isEmpty()) ? u : null;
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
                // continue trying next method
            }
        }
        return null;
    }
}
