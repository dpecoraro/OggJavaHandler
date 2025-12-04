package com.santander.goldengate.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
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
import org.apache.kafka.common.serialization.StringSerializer;

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
    private KafkaProducer<String, GenericRecord> kafkaProducer; // use Avro serializer for values
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
            this.schemaManager = new AvroSchemaManager(namespacePrefix);

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

            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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

            processOperation(operation, tx);
            return Status.OK;

        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error in operationAdded: " + ex.getMessage());
            return Status.CHKPT;
        }
    }

    private void processOperation(DsOperation operation, DsTransaction tx) {
        if (tx == null || operation == null) {
            System.out.println(">>> [KcopHandler] Warning: tx/operation null");
            return;
        }

        final String table = operation.getTableName() != null ? operation.getTableName().toString() : "UNKNOWN";
        // Map GoldenGate operation to CDC code
        final String opType = mapEntTyp(operation); // changed

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
                if (afterVal != null) afterImage.put(columnName, extractValue(afterVal));
                Object beforeVal = c.hasBeforeValue() ? c.getBeforeValue() : null;
                if (beforeVal != null) beforeImage.put(columnName, extractValue(beforeVal));
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

            cdcRecord.put("A_ENTTYP", opType); // now uses mapped value
            cdcRecord.put("A_CCID", tx.getTranID() != null ? tx.getTranID().toString() : null);
            cdcRecord.put("A_TIMSTAMP", String.valueOf(System.currentTimeMillis()));
            cdcRecord.put("A_JOBUSER", System.getProperty("user.name"));
            cdcRecord.put("A_USER", System.getProperty("user.name"));

            // No manual Avro serialization; KafkaAvroSerializer handles it
            if (kafkaProducer == null) {
                System.err.println("[KcopHandler] Kafka producer not initialized, skipping send.");
                return;
            }

            final String topic = resolveTopic(topicMappingTemplate, table);
            final String key = buildKey(tx);

            // Register schemas once per topic (value and key)
            if (lastRegisteredTopic == null || !lastRegisteredTopic.equals(topic)) {
                schemaRegistryClient.registerIfNeeded(topic + "-value", avroSchema);
                // register STRING schema for key
                schemaRegistryClient.registerIfNeeded(topic + "-key", Schema.create(Type.STRING)); // added
                lastRegisteredTopic = topic;
            }

            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, key, cdcRecord);
            System.out.println(">>> [KcopHandler] Sending bootstrap=" + kafkaBootstrapServers
                    + " topic=" + topic + " key=" + key);

            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[KcopHandler] Kafka send error: " + exception.getMessage());
                } else {
                    System.out.println(">>> [KcopHandler] Sent topic=" + metadata.topic()
                            + " partition=" + metadata.partition()
                            + " offset=" + metadata.offset());
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

    // Build key from transaction
    protected String buildKey(DsTransaction tx) {
        return tx != null && tx.getTranID() != null ? tx.getTranID().toString() : "unknown";
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
        if (value == null) return schemaManager.getDefaultValue(schema);
        Type type = schema.getType();
        try {
            switch (type) {
                case INT:
                    if (value instanceof Number) return ((Number) value).intValue();
                    return Integer.valueOf(value.toString().trim());
                case LONG:
                    if (value instanceof Number) return ((Number) value).longValue();
                    String ls = value.toString().trim();
                    if (ls.contains(".")) {
                        double d = Double.parseDouble(ls);
                        return (long) Math.round(d);
                    }
                    return Long.valueOf(ls);
                case FLOAT:
                    if (value instanceof Number) return ((Number) value).floatValue();
                    return Float.valueOf(value.toString().trim());
                case DOUBLE:
                    if (value instanceof Number) return ((Number) value).doubleValue();
                    return Double.valueOf(value.toString().trim());
                case STRING: {
                    String s = value.toString();
                    String logical = schema.getProp("logicalType");

                    // Normalize DATE to yyyy-MM-dd
                    if (logical != null && "DATE".equalsIgnoreCase(logical)) {
                        int spaceIdx = s.indexOf(' ');
                        int tIdx = s.indexOf('T');
                        int cutIdx = (spaceIdx > 0) ? spaceIdx : (tIdx > 0 ? tIdx : -1);
                        String dateOnly = cutIdx > 0 ? s.substring(0, cutIdx) : s;
                        if (dateOnly.length() >= 10) dateOnly = dateOnly.substring(0, 10);
                        return dateOnly;
                    }

                    // Normalize TIMESTAMP to ISO with 'T' and 18-digit fractional seconds
                    if (logical != null && "TIMESTAMP".equalsIgnoreCase(logical)) {
                        // Replace space with 'T' if present
                        String iso = s.replace(' ', 'T');

                        int dotIdx = iso.indexOf('.');
                        if (dotIdx < 0) {
                            // No fractional part: append 18 zeros
                            iso = iso + ".000000000000000000";
                        } else {
                            // Pad or trim fractional seconds to 18 digits
                            int endIdx = iso.indexOf('Z') > 0 ? iso.indexOf('Z') : iso.length();
                            String prefix = iso.substring(0, dotIdx + 1);
                            String fracAndRest = iso.substring(dotIdx + 1, endIdx);
                            // Keep only digits in fractional part
                            StringBuilder digits = new StringBuilder();
                            for (int i = 0; i < fracAndRest.length(); i++) {
                                char c = fracAndRest.charAt(i);
                                if (Character.isDigit(c)) digits.append(c);
                                else break; // stop at first non-digit
                            }
                            String frac = digits.toString();
                            if (frac.length() > 18) {
                                frac = frac.substring(0, 18);
                            } else if (frac.length() < 18) {
                                StringBuilder pad = new StringBuilder(frac);
                                while (pad.length() < 18) pad.append('0');
                                frac = pad.toString();
                            }
                            // Rebuild timestamp with padded fraction and any remainder after fraction
                            String remainder = iso.substring(dotIdx + 1 + digits.length());
                            iso = prefix + frac + remainder;
                        }
                        return iso;
                    }

                    return s;
                }
                case BYTES:
                    if (value instanceof byte[]) return ByteBuffer.wrap((byte[]) value);
                    if (value instanceof ByteBuffer) return value;
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
            return schemaManager.getDefaultValue(schema);
        }
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
            if (value == null) return null;
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
        if (tableMetaData == null || index < 0) return null;
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

    // Map GoldenGate operation enum/name to CDC short codes
    private String mapEntTyp(DsOperation operation) {
        if (operation == null || operation.getOperationType() == null) return "UN";
        String name = operation.getOperationType().name();
        // Handle typical GG names like DO_INSERT, DO_UPDATE, DO_DELETE, DO_UNIFIED_UPDATE_VAL, etc.
        if (name.contains("INSERT")) return "IN";
        if (name.contains("UPDATE")) return "UP";
        if (name.contains("DELETE")) return "DL";
        return "UN";
    }
}
