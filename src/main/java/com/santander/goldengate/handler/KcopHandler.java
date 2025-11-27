package com.santander.goldengate.handler;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
    private Map<String, Schema> schemaCache = new HashMap<>();
    private KafkaProducer<String, byte[]> kafkaProducer;
    private String topicMappingTemplate; 
    private String kafkaBootstrapServers;
    // Schema Registry (multi-URL)
    private List<String> schemaRegistryUrls = new ArrayList<>();
    private final Set<String> registeredSubjects = new HashSet<>();

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
                // Default properties
                kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
            }
            // Read topic template from properties (Replicat/handler properties)
            this.topicMappingTemplate = kafkaProps.getProperty("gg.handler.kafkahandler.topicMappingTemplate");
            this.kafkaBootstrapServers = kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            // Read Schema Registry URLs (value first, then key) and split by comma
            String valueUrls = kafkaProps.getProperty("value.converter.schema.registry.url");
            String keyUrls = kafkaProps.getProperty("key.converter.schema.registry.url");
            String rawUrls = (valueUrls != null && !valueUrls.isEmpty()) ? valueUrls : keyUrls;

            if (rawUrls != null && !rawUrls.isEmpty()) {
                for (String u : rawUrls.split(",")) {
                    String trimmed = u.trim();
                    if (!trimmed.isEmpty()) schemaRegistryUrls.add(trimmed);
                }
            }

            // Force correct serializers (avoid external misconfiguration)
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            kafkaProducer = new KafkaProducer<>(kafkaProps);
            System.out.println(">>> [KcopHandler] Kafka Producer initialized");
            System.out.println(">>> [KcopHandler] Kafka bootstrap.servers: " + kafkaBootstrapServers);
            if (topicMappingTemplate != null) {
                System.out.println(">>> [KcopHandler] Topic template: " + topicMappingTemplate);
            }
            if (!schemaRegistryUrls.isEmpty()) {
                System.out.println(">>> [KcopHandler] Schema Registry URLs: " + String.join(", ", schemaRegistryUrls));
            } else {
                System.out.println(">>> [KcopHandler] Schema Registry URLs not configured; skipping registration.");
            }
        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error initializing Kafka Producer: " + ex.getMessage());
            ex.printStackTrace();
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
            ex.printStackTrace();
            return Status.OK;
        }
    }

    private void processOperation(DsOperation operation, DsTransaction tx) {
        if (tx == null) {
            System.out.println(">>> [KcopHandler] Warning: tx null");
            return;
        }

        String table = operation.getTableName() != null ? operation.getTableName().toString() : "UNKNOWN";
        String opType = operation.getOperationType().name();

        //System.out.println(">>> [KcopHandler] Retrieved metadata for table " + table); 
        // Get table metadata
        TableMetaData tableMetaData = null;
        if (metaData != null && operation.getTableName() != null) {
            tableMetaData = metaData.getTableMetaData(operation.getTableName());
            //System.out.println(">>> [KcopHandler] Retrieved metadata: " + tableMetaData); 
        }

        Map<String, Object> beforeImage = new LinkedHashMap<>();
        Map<String, Object> afterImage = new LinkedHashMap<>();

        DsRecord record = operation.getRecord();
        if (record == null || record.getColumns() == null) {
            System.out.println(">>> [KcopHandler] Warning: record/columns null for table " + table);
        } else {
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
        }

        try {
            // Build Avro schema (cached per table)
            Schema avroSchema = getOrCreateAvroSchema(table, tableMetaData);
            
            // Create GenericRecord
            GenericRecord cdcRecord = new GenericData.Record(avroSchema);
            
            // Populate beforeImage
            if (!beforeImage.isEmpty()) {
                GenericRecord beforeRec = createTableRecord(avroSchema, "beforeImage", beforeImage);
                cdcRecord.put("beforeImage", beforeRec);
            } else {
                cdcRecord.put("beforeImage", null);
            }
            
            // Populate afterImage
            if (!afterImage.isEmpty()) {
                GenericRecord afterRec = createTableRecord(avroSchema, "afterImage", afterImage);
                cdcRecord.put("afterImage", afterRec);
            } else {
                cdcRecord.put("afterImage", null);
            }
            
            // Metadata fields - convert all to String
            cdcRecord.put("A_ENTTYP", opType);
            cdcRecord.put("A_CCID", tx.getTranID() != null ? tx.getTranID().toString() : null);
            cdcRecord.put("A_TIMSTAMP", String.valueOf(System.currentTimeMillis()));
            cdcRecord.put("A_JOBUSER", System.getProperty("user.name"));
            cdcRecord.put("A_USER", System.getProperty("user.name"));
            
            // Serialize to Avro binary
            byte[] avroBytes = serializeAvro(avroSchema, cdcRecord);

            if (kafkaProducer == null) {
                System.err.println("[KcopHandler] Kafka producer not initialized, skipping send.");
                return;
            }

            String fullyQualifiedTableName = table; 
            String topic = resolveTopic(topicMappingTemplate, fullyQualifiedTableName);
            String key = tx.getTranID().toString(); 

            // Register schema (subject = <topic>-value) against first working registry
            String subject = topic + "-value";
            ensureSchemaRegistered(subject, avroSchema);

            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, avroBytes);
            System.out.println(">>> [KcopHandler] Sending to Kafka bootstrap=" + kafkaBootstrapServers 
                + " topic=" + topic + " key=" + key + " size=" + avroBytes.length + "B");

            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[KcopHandler] Error sending to Kafka: " + exception.getMessage());
                } else {
                    System.out.println(">>> [KcopHandler] Sent to Kafka bootstrap=" + kafkaBootstrapServers
                        + " topic=" + metadata.topic() 
                        + " partition=" + metadata.partition() 
                        + " offset=" + metadata.offset());
                }
            });
            
            System.out.println(">>> SCHEMA: " + avroSchema.toString(true));
            System.out.println(">>> CDC Record: " + cdcRecord);
            System.out.println(">>> Avro Binary Size: " + avroBytes.length + " bytes");
            
        } catch (Exception ex) {
            System.err.println("[KcopHandler] Error creating Avro record: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    // Ensure schema is registered once per subject
    private void ensureSchemaRegistered(String subject, Schema schema) {
        if (schema == null || subject == null || schemaRegistryUrls.isEmpty()) return;
        if (registeredSubjects.contains(subject)) return;
        try {
            int id = registerSchema(subject, schema);
            if (id > 0) {
                registeredSubjects.add(subject);
                System.out.println(">>> [KcopHandler] Registered schema subject=" + subject + " id=" + id);
            } else {
                System.out.println(">>> [KcopHandler] Schema registration returned no id for subject=" + subject);
            }
        } catch (Exception e) {
            System.err.println("[KcopHandler] Failed to register schema for subject=" + subject + ": " + e.getMessage());
        }
    }

    // Try all configured registries until one succeeds
    private int registerSchema(String subject, Schema schema) throws Exception {
        Exception last = null;
        for (String base : schemaRegistryUrls) {
            try {
                int id = postSchemaToRegistry(base, subject, schema);
                if (id > 0) return id;
            } catch (Exception e) {
                last = e;
                System.err.println("[KcopHandler] Schema Registry POST failed for " + base + ": " + e.getMessage());
            }
        }
        if (last != null) throw last;
        return 0;
    }

    // POST {"schema":"<json>"} to /subjects/{subject}/versions
    private int postSchemaToRegistry(String baseUrl, String subject, Schema schema) throws Exception {
        String endpoint = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        endpoint += "subjects/" + URLEncoder.encode(subject, "UTF-8") + "/versions";
        
        System.out.println(">>> [KcopHandler] Registering schema to " + endpoint);

        String schemaJson = schema.toString();
        String escaped = schemaJson.replace("\\", "\\\\").replace("\"", "\\\"");
        String payload = "{\"schema\":\"" + escaped + "\"}";

        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(10000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
        conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.getBytes(StandardCharsets.UTF_8));
        }

        int code = conn.getResponseCode();
        InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
        StringBuilder sb = new StringBuilder();
        if (is != null) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) sb.append(line);
            }
        }
        String body = sb.toString();
        if (code >= 200 && code < 300) {
            System.out.println(">>> [KcopHandler] Schema Registry response: " + body);
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"id\"\\s*:\\s*(\\d+)").matcher(body);
            if (m.find()) return Integer.parseInt(m.group(1));
            return 0;
        } else {
            throw new RuntimeException("Schema Registry error (" + code + "): " + body);
        }
    }

    private Schema getOrCreateAvroSchema(String tableName, TableMetaData tableMetaData) {
        //System.out.println(">>> [KcopHandler] Generating Avro schema for table " + tableName);
        if (schemaCache.containsKey(tableName)) {
            return schemaCache.get(tableName);
        }
        
        String namespace = "value." + tableName;
        String tableRecordName = tableName.contains(".") ? tableName.substring(tableName.lastIndexOf('.') + 1) : tableName;
        
        // Build table record schema with proper types
        List<Field> tableFields = new ArrayList<>();
        int idx = 0;
        while (tableMetaData != null) {
            ColumnMetaData cm = safeGetColumnMetaData(tableMetaData, idx);
            if (cm == null) break;
            
            String colName = cm.getColumnName();
            Schema colSchema = buildColumnSchema(cm);
            
            // Get default value based on type
            Object defaultValue = getDefaultValue(colSchema);
            
            Field field = new Field(colName, colSchema, "", defaultValue);
            tableFields.add(field);
            idx++;
        }
        
        Schema tableSchema = Schema.createRecord(tableRecordName, "", namespace, false, tableFields);
        
        // Build envelope schema
        List<Field> envelopeFields = new ArrayList<>();
        envelopeFields.add(new Field("beforeImage", Schema.createUnion(Schema.create(Type.NULL), tableSchema), "", null));
        envelopeFields.add(new Field("afterImage", Schema.createUnion(Schema.create(Type.NULL), tableSchema), "", null));
        envelopeFields.add(new Field("A_ENTTYP", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING)), "", null));
        envelopeFields.add(new Field("A_CCID", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING)), "", null));
        envelopeFields.add(new Field("A_TIMSTAMP", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING)), "", null));
        envelopeFields.add(new Field("A_JOBUSER", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING)), "", null));
        envelopeFields.add(new Field("A_USER", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.STRING)), "", null));
        
        Schema envelopeSchema = Schema.createRecord("AuditRecord", "", namespace, false, envelopeFields);
        
        schemaCache.put(tableName, envelopeSchema);
        return envelopeSchema;
    }

    private Schema buildColumnSchema(ColumnMetaData cm) {
        /*System.out.println(">>> [KcopHandler] Building column schema for column " + cm.getColumnName() 
        + " with data type " 
        + cm.getDataType()); */
        
        String colName = cm.getColumnName();
        String dataTypeName = cm.getDataType() != null ? cm.getDataType().toString().toUpperCase() : "STRING";
        String dataTypeRaw  = cm.getDataType() != null ? cm.getDataType().toString() : "";

        int precision = -1;
        int scale = -1;
        // Parse patterns like NUMBER(15,2), DECIMAL(10,0), NUMERIC(8)
        Matcher m = Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)").matcher(dataTypeRaw);
        if (m.find()) {
            try {
                precision = Integer.parseInt(m.group(1));
                if (m.group(2) != null) scale = Integer.parseInt(m.group(2));
            } catch (NumberFormatException ignore) {}
        }

        Schema baseSchema;
        String logicalType;

        if (dataTypeName.contains("NUMBER") || dataTypeName.contains("DECIMAL") || dataTypeName.contains("NUMERIC")) {
            if (scale > 0) {
                baseSchema = Schema.create(Type.DOUBLE);
            } else {
                baseSchema = Schema.create(Type.LONG);
            }
            logicalType = "DECIMAL";
        } else if (dataTypeName.contains("INT") || dataTypeName.contains("SMALLINT") || dataTypeName.contains("BIGINT")) {
            if (dataTypeName.contains("BIGINT")) baseSchema = Schema.create(Type.LONG);
            else baseSchema = Schema.create(Type.INT);
            logicalType = "DECIMAL";
        } else if (dataTypeName.contains("FLOAT") || dataTypeName.contains("DOUBLE") || dataTypeName.contains("REAL")) {
            baseSchema = Schema.create(Type.DOUBLE);
            logicalType = "DOUBLE";
        } else if (dataTypeName.contains("DATE")) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "DATE";
        } else if (dataTypeName.contains("TIME")) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = dataTypeName.contains("TIMESTAMP") ? "TIMESTAMP" : "TIME";
        } else if (dataTypeName.contains("CHAR") || dataTypeName.contains("VARCHAR") || dataTypeName.contains("TEXT")) {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "CHARACTER";
        } else if (dataTypeName.contains("BLOB") || dataTypeName.contains("BINARY") || dataTypeName.contains("VARBINARY")) {
            baseSchema = Schema.create(Type.BYTES);
            logicalType = "BINARY";
        } else {
            baseSchema = Schema.create(Type.STRING);
            logicalType = "CHARACTER";
        }

        baseSchema.addProp("logicalType", logicalType);
        baseSchema.addProp("dbColumnName", colName);

        if ("DECIMAL".equals(logicalType)) {
            baseSchema.addProp("precision", precision > 0 ? precision : 15);
            baseSchema.addProp("scale", scale >= 0 ? scale : 0);
        }

        if (logicalType.equals("CHARACTER") || logicalType.equals("TIMESTAMP") ||
            logicalType.equals("DATE") || logicalType.equals("TIME")) {
            int length = logicalType.equals("TIMESTAMP") ? 32 :
                         logicalType.equals("DATE") ? 10 :
                         logicalType.equals("TIME") ? 8 : 255;
            baseSchema.addProp("length", length);
        }

        return baseSchema;
    }

    private Object getDefaultValue(Schema schema) {
        Type type = schema.getType();
        switch (type) {
            case INT:
            case LONG: return 0;
            case FLOAT:
            case DOUBLE: return 0.0;
            case BOOLEAN: return false;
            case STRING: return "";
            case BYTES: return ByteBuffer.wrap(new byte[0]); // fixed
            default: return null;
        }
    }

    private GenericRecord createTableRecord(Schema envelopeSchema, String fieldName, Map<String, Object> data) {
        Schema unionSchema = envelopeSchema.getField(fieldName).schema();
        Schema tableSchema = unionSchema.getTypes().get(1); // Get non-null type from union
        
        GenericRecord tableRecord = new GenericData.Record(tableSchema);
        for (Field field : tableSchema.getFields()) {
            Object value = data.get(field.name());
            // Convert value to match schema type
            System.out.println(">>> [KcopHandler] Converting value to match schema type for field " + field.name() + ": " + value);
            Object convertedValue = convertValueToSchemaType(value, field.schema());
            tableRecord.put(field.name(), convertedValue);
        }
        return tableRecord;
    }

    private Object convertValueToSchemaType(Object value, Schema schema) {
        if (value == null) return getDefaultValue(schema);
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
                case STRING:
                    return value.toString();
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
            return getDefaultValue(schema);
        }
    }

    private byte[] serializeAvro(Schema schema, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
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

    private Object extractValue(Object value) {
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
        /*if (tx != null) {
            System.out.println(">>> [KcopHandler] COMMIT TX=" + tx.getTranID());
        }*/
        return Status.OK;
    }

    @Override
    public void destroy() {
        System.out.println(">>> [KcopHandler] destroy() called");
        //System.out.println(">>> [KcopHandler] Total operations processed: " + operationCount);
        
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
    private String resolveTopic(String template, String fullyQualifiedTableName) {
        if (template == null || template.isEmpty()) {
            // fallback to previous default if no template provided
            return "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_");
        }
        return template.replace("${fullyQualifiedTableName}", fullyQualifiedTableName);
    }
}
