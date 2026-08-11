package com.santander.goldengate.handler;

import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
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

import com.santander.goldengate.helpers.ColumnSchemaMapper;
import com.santander.goldengate.helpers.DateFormatHandler;
import com.santander.goldengate.helpers.DecimalValueConverter;
import com.santander.goldengate.helpers.EntityTypeFormatHandler;
import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.AbstractHandler;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsColumn.BeforeAfter;
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

    private static final Logger LOGGER = Logger.getLogger(KcopHandler.class.getName());
    private static final String LOG_LEVEL = "gg.handler.kcoph.logLevel";
    private static final String STATUS_LOG_INTERVAL = "gg.handler.kcoph.statusLogInterval";
    private static final long DEFAULT_STATUS_LOG_INTERVAL = 10_000L;

    private final AtomicLong operationCount = new AtomicLong();
    private final ColumnSchemaMapper columnSchemaMapper = new ColumnSchemaMapper();
    private final DecimalValueConverter decimalValueConverter = new DecimalValueConverter();
    private final Map<String, Schema> finalSchemaCache = new HashMap<>();
    private final Map<String, Schema> keySchemaCache = new HashMap<>();
    private final Set<String> registeredTopics = new HashSet<>();
    private long statusLogInterval = DEFAULT_STATUS_LOG_INTERVAL;
    private String kafkaProducerConfigFile;
    private DsMetaData metaData;
    private AvroSchemaManager schemaManager;
    private SchemaTypeConverter schemaTypeConverter = new SchemaTypeConverter();
    private KafkaProducer<String, GenericRecord> kafkaProducer; 
    private String topicMappingTemplate;
    private String kafkaBootstrapServers;
    private String namespacePrefix;

    private SchemaRegistryClient schemaRegistryClient;
    private Db2SchemaContractCatalog schemaContractCatalog;
    private DateFormatHandler dateFormatHandler = new DateFormatHandler();

    private Map<String, String[]> keyColumnsOverrides = new HashMap<>();
    private Map<String, LinkedHashMap<String, Integer>> defaultKeyColumnSpecs = new HashMap<>();

    private static final class KeyFieldSpec {
        private final int keyIndex;
        private final int tableIndex;
        private final String columnName;
        private final Schema schema;

        private KeyFieldSpec(int keyIndex, int tableIndex, String columnName, Schema schema) {
            this.keyIndex = keyIndex;
            this.tableIndex = tableIndex;
            this.columnName = columnName;
            this.schema = schema;
        }
    }

    public KcopHandler() {
    }

    public void setKafkaProducerConfigFile(String kafkaProducerConfigFile) {
        this.kafkaProducerConfigFile = kafkaProducerConfigFile;
        //System.out.println(">>> [KcopHandler] kafkaProducerConfigFile set to " + kafkaProducerConfigFile);
    }

    public void setTopicMappingTemplate(String topicMappingTemplate) {
        this.topicMappingTemplate = topicMappingTemplate;
        //System.out.println(">>> [KcopHandler] topicMappingTemplate set to " + topicMappingTemplate);
    }

    public void setNameSpacePrefix(String namespacePrefix) {
        this.namespacePrefix = namespacePrefix;
        //System.out.println(">>> [KcopHandler] namespacePrefix set to " + namespacePrefix);
    }

    @Override
    public void init(DsConfiguration config, DsMetaData metaData) {
        //System.out.println(">>> [KcopHandler] init() called");
        super.init(config, metaData);
        this.metaData = metaData;

        // Initialize Kafka Producer
        try {
            Properties kafkaProps = new Properties();
            if (kafkaProducerConfigFile != null) {
                try (FileInputStream fis = new FileInputStream(kafkaProducerConfigFile)) {
                    kafkaProps.load(fis);
                    //System.out.println(">>> [KcopHandler] Loaded Kafka producer properties from " + kafkaProducerConfigFile);
                }
            } else {
                throw new NoSuchAttributeException("lack of kafka producer config file");
            }
            // Read topic template and bootstrap
            //this.topicMappingTemplate = kafkaProps.getProperty("gg.handler.kcoph.topicMappingTemplate");
            this.kafkaBootstrapServers = kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            LOGGER.setLevel(parseLogLevel(kafkaProps.getProperty(LOG_LEVEL, "INFO")));
            kafkaProps.remove(LOG_LEVEL);
            this.statusLogInterval = Long.parseLong(
                    kafkaProps.getProperty(STATUS_LOG_INTERVAL,
                            String.valueOf(DEFAULT_STATUS_LOG_INTERVAL)));
            kafkaProps.remove(STATUS_LOG_INTERVAL);

            // Namespace prefix and schema manager
            //String namespacePrefix = kafkaProps.getProperty("gg.handler.kcoph.namespacePrefix", "value.SOURCEDB.BALP");
            this.schemaTypeConverter = new SchemaTypeConverter();
            this.schemaManager = new AvroSchemaManager(namespacePrefix, columnSchemaMapper);
            this.schemaContractCatalog = Db2SchemaContractCatalog.loadBundled();

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
                if (propName.startsWith("gg.handler.kafkahandler.keyColumns")) {
                    String tableCode = propName.substring(propName.lastIndexOf('.') + 1).toUpperCase();
                    String raw = kafkaProps.getProperty(propName, "");
                    String[] cols = Arrays.stream(raw.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .toArray(String[]::new);
                    if (cols.length > 0) {
                        keyColumnsOverrides.put(tableCode, cols);
                        //System.out.println(">>> [KcopHandler] Key columns override loaded for " + tableCode + ": " + Arrays.toString(cols));
                    }
                }
                //System.out.println(">>> [KcopHandler] Key columns override keys: " + keyColumnsOverrides.keySet());
            }

            kafkaProducer = new KafkaProducer<>(kafkaProps);
            LOGGER.info(() -> "Kafka producer initialized: bootstrap=" + kafkaBootstrapServers
                    + ", db2ValueContracts=" + schemaContractCatalog.valueSchemaCount()
                    + ", db2KeyContracts=" + schemaContractCatalog.keySchemaCount());
            //System.out.println(">>> [KcopHandler] Kafka bootstrap.servers: " + kafkaBootstrapServers);
            //System.out.println(">>> [KcopHandler] Namespace prefix: " + namespacePrefix);
            if (topicMappingTemplate != null) {
                //System.out.println(">>> [KcopHandler] Topic template: " + topicMappingTemplate);
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error initializing Kafka producer", ex);
        }
    }

    @Override
    public Status operationAdded(DsEvent event, DsTransaction tx, DsOperation operation) {
        try {
            if (operation == null) {
                return Status.OK;
            }

            long received = operationCount.incrementAndGet();
            if (statusLogInterval > 0 && received % statusLogInterval == 0) {
                LOGGER.info(this::reportStatus);
            }

            // pass event to processOperation
            processOperation(event, tx, operation);
            return Status.OK;

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Operation could not be queued for Kafka", ex);
            return OperationDeliverySupport.failureStatus();
        }
    }

    // include event to read operation timestamp
    private void processOperation(DsEvent event, DsTransaction tx, DsOperation operation) throws Exception {
        if (tx == null || operation == null) {
            //System.out.println(">>> [KcopHandler] Warning: tx/operation null");
            return;
        }

        String table = operation.getTableName() != null ? operation.getTableName().toString() : "UNKNOWN";

        EntityTypeFormatHandler enttypHandler = new EntityTypeFormatHandler();
        String opType = enttypHandler.mapEntTyp(operation);

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
                if (c.hasAfterValue()) {
                    afterImage.put(columnName, extractColumnValue(c, BeforeAfter.AFTER));
                }
                if (c.hasBeforeValue()) {
                    beforeImage.put(columnName, extractColumnValue(c, BeforeAfter.BEFORE));
                }
                idx++;
            }
        } else {
            //System.out.println(">>> [KcopHandler] Warning: record/columns null for table " + table);
        }

        try {
            Schema avroSchemaFixed = finalSchemaCache.get(table);
            if (avroSchemaFixed == null) {
                Schema referenceSchema = schemaContractCatalog.valueSchema(table);
                if (referenceSchema != null) {
                    avroSchemaFixed = referenceSchema;
                } else {
                    Schema avroSchema = schemaManager.getOrCreateAvroSchema(table, tableMetaData);
                    avroSchemaFixed = schemaTypeConverter.rebuildEnvelopeWithClonedTableSchema(
                            avroSchema, tableMetaData);
                }
                finalSchemaCache.put(table, avroSchemaFixed);
            }
            //System.out.println(">>> [KcopHandler] Using Avro schema: " + avroSchemaFixed.getFields());

            GenericRecord cdcRecord;
            if (avroSchemaFixed.getField("beforeImage") != null
                    || avroSchemaFixed.getField("afterImage") != null) {
                cdcRecord = new GenericData.Record(avroSchemaFixed);
                cdcRecord.put("beforeImage", beforeImage.isEmpty()
                        ? null
                        : createTableRecord(avroSchemaFixed, "beforeImage", beforeImage));
                cdcRecord.put("afterImage", afterImage.isEmpty()
                        ? null
                        : createTableRecord(avroSchemaFixed, "afterImage", afterImage));
            } else {
                Map<String, Object> currentImage = afterImage.isEmpty() ? beforeImage : afterImage;
                cdcRecord = createRecord(avroSchemaFixed, currentImage);
            }

            putIfPresent(cdcRecord, "A_ENTTYP", opType);
            putIfPresent(cdcRecord, "A_CCID",
                    tx.getTranID() != null ? tx.getTranID().toString() : null);
            putIfPresent(cdcRecord, "A_TIMSTAMP", dateFormatHandler.formatMillisSpace12(
                    extractOperationTimestampMillis(event, tx, operation)));

            //String ggUser = extractUser(event, tx, operation);
            //cdcRecord.put("A_JOBUSER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser); // changed
            //cdcRecord.put("A_USER", ggUser != null && !ggUser.isEmpty() ? ggUser : sysUser);    // changed
            // Build topic
            //System.out.println(">>> [KcopHandler] Building topic");
            String topic = resolveTopic(topicMappingTemplate, table);

            // Build Avro key schema (RECORD) and key GenericRecord from PK columns
            //System.out.println(">>> [KcopHandler] Building KeySchema");
            Schema keySchema = keySchemaCache.get(table);
            if (keySchema == null) {
                keySchema = buildRecordKeySchema(table, tableMetaData);
                keySchemaCache.put(table, keySchema);
            }
            //System.out.println(">>> [KcopHandler] Building KeyString");
            String keyRecord = buildKeyString(table, keySchema, cdcRecord);

            /*System.out.println(">>> [KcopHandler] Prepared message:"
                    + " topic=" + topic
                    + " keyRecord=" + keyRecord
                    + " keySchema=" + keySchema.getFullName()
                    + " A_ENTTYP=" + cdcRecord.get("A_ENTTYP")
                    + " A_CCID=" + cdcRecord.get("A_CCID")
                    + " A_TIMSTAMP=" + cdcRecord.get("A_TIMSTAMP")
                    + " A_JOBUSER=" + cdcRecord.get("A_JOBUSER")
                    + " A_USER=" + cdcRecord.get("A_USER")); */

            // Register schemas once per topic (value and key) — RECORD key
            if (!registeredTopics.contains(topic)) {
                String valueSubject = topic + "-value";
                String keySubject = topic + "-key";

                /*System.out.println(">>> [KcopHandler] Registering value schema:"
                        + " subject=" + valueSubject
                        + " schemaName=" + avroSchemaFixed.getFullName());*/
                schemaRegistryClient.registerIfNeeded(valueSubject, avroSchemaFixed);

                /*(System.out.println(">>> [KcopHandler] Registering key schema:"
                        + " subject=" + keySubject
                        + " schema=" + keySchema.toString());*/
                schemaRegistryClient.registerIfNeeded(keySubject, keySchema); 

                registeredTopics.add(topic);
                LOGGER.info(() -> "Schema Registry subjects registered: valueSubject="
                        + valueSubject + ", keySubject=" + keySubject);
            }

            //System.out.println(">>> [KcopHandler] Envelope schema (pretty): " + avroSchemaFixed.toString(true));
            //System.out.println(">>> [KcopHandler] CDC Record payload: " + cdcRecord);
            //System.out.println(">>> [KcopHandler] Key Record payload: " + keyRecord);
            //System.out.println(">>> [KcopHandler] BeforeImage map: " + beforeImage);
            //System.out.println(">>> [KcopHandler] AfterImage map: " + afterImage);

            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, keyRecord, cdcRecord);
            /*System.out.println(">>> [KcopHandler] Sending to Kafka: bootstrap=" + kafkaBootstrapServers
                    + " topic=" + topic
                    + " key.schema=" + keySchema.getFullName());*/

            sendAndAwait(producerRecord);

            //System.out.println(">>> SCHEMA: " + avroSchemaFixed.toString(true));
            //System.out.println(">>> CDC Record: " + cdcRecord);
        } catch (Exception ex) {
            throw ex;
        }
    }

    // Create a table image record ("beforeImage" or "afterImage") using the envelope schema
    private GenericRecord createTableRecord(Schema envelopeSchema, String fieldName, Map<String, Object> image) {
        if (envelopeSchema == null || fieldName == null) {
            return null;
        }
        Field field = envelopeSchema.getField(fieldName);
        if (field == null) {
            throw new IllegalArgumentException("Envelope schema missing field: " + fieldName);
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
            throw new IllegalArgumentException("Field " + fieldName + " is not a RECORD schema");
        }

        return createRecord(fieldSchema, image);
    }

    private GenericRecord createRecord(Schema recordSchema, Map<String, Object> image) {
        GenericRecord rec = new GenericData.Record(recordSchema);
        for (Schema.Field colField : recordSchema.getFields()) {
            String colName = colField.name();
            boolean valuePresent = image != null && image.containsKey(colName);
            Object raw = valuePresent ? image.get(colName) : null;
            Object converted;
            if (valuePresent && raw == null) {
                converted = OperationDeliverySupport.resolveSqlNull(colField);
            } else if (!valuePresent && colField.hasDefaultValue()) {
                converted = GenericData.get().getDefaultValue(colField);
            } else {
                converted = convertValueToSchemaType(raw, colField.schema(), colName);
            }
            rec.put(colName, converted);
        }
        return rec;
    }

    private void putIfPresent(GenericRecord record, String fieldName, Object value) {
        if (record.getSchema().getField(fieldName) != null) {
            record.put(fieldName, value);
        }
    }

    // Value conversion with logical types support (DATE/TIMESTAMP/DECIMAL)
    protected Object convertValueToSchemaType(Object value, Schema schema, String fieldName) {
        if (value == null) {
            return allowsNull(schema) ? null : schemaTypeConverter.getDefaultValue(schema);
        }
        Schema effectiveSchema = schemaTypeConverter.nonNullSchema(schema);
        String logical = effectiveSchema.getProp("logicalType");
        Object out;

        try {
            // DECIMAL
            boolean isDecimalLogical = logical != null && "DECIMAL".equalsIgnoreCase(logical);
            if (isDecimalLogical) {
                return decimalValueConverter.convert(value, effectiveSchema, fieldName);
            }

            out = convertValueToSchemaType(value, effectiveSchema);

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
                    } else {
                        break;
                    }
                }
                String frac = digits.toString();
                if (frac.length() > 12) {
                    frac = frac.substring(0, 12);
                } else {
                    while (frac.length() < 12) {
                        frac += '0';
                    }
                }
                String remainder = iso.substring(dotIdx + 1 + digits.length(), endIdx);
                return prefix + frac + remainder + (endIdx < iso.length() ? iso.substring(endIdx) : "");
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("Cannot convert field " + fieldName
                    + " to schema " + effectiveSchema.getType(), ex);
        }

        return out;
    }

    // Base conversion by Avro primitive type
    protected Object convertValueToSchemaType(Object value, Schema schema) {
        if (value == null) {
            return allowsNull(schema) ? null : schemaTypeConverter.getDefaultValue(schema);
        }
        Schema effectiveSchema = schemaTypeConverter.nonNullSchema(schema);
        Type type = effectiveSchema.getType();
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
            throw new IllegalArgumentException(
                    "Cannot convert field value to Avro type " + type, e);
        }
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

    Object extractColumnValue(DsColumn column, BeforeAfter image) {
        return OperationDeliverySupport.extractColumnValue(column, image);
    }

    void sendAndAwait(ProducerRecord<String, GenericRecord> producerRecord) throws Exception {
        OperationDeliverySupport.await(kafkaProducer.send(producerRecord));
    }

    private boolean allowsNull(Schema schema) {
        return schemaTypeConverter.allowsNull(schema);
    }

    // Build RECORD key schema based on PK columns (or overrides or defaults)
    private Schema buildRecordKeySchema(String table, TableMetaData tableMetaData) {
        Schema referenceSchema = schemaContractCatalog.keySchema(table);
        if (referenceSchema != null) {
            return referenceSchema;
        }
        String shortName = table != null && table.contains(".")
                ? table.substring(table.lastIndexOf('.') + 1)
                : table;
        String tableUpper = shortName != null ? shortName.toUpperCase() : "TABLE";
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                .record(tableUpper) 
                .namespace("key.SOURCEDB.BALP")
                .fields();
        String[] overrideCols = keyColumnsOverrides.get(tableUpper);
        try {
            if (overrideCols != null && overrideCols.length > 0) {
                for (String colName : overrideCols) {
                    ColumnMetaData col = schemaTypeConverter.findColumnByName(tableMetaData, colName);
                    if (col == null) {
                        throw new IllegalArgumentException("Unknown key override column " + colName
                                + " for table " + tableUpper);
                    }
                    ColumnSchemaMapper.Mapping mapping = columnSchemaMapper.map(col);
                    fields.name(colName).doc("").type(mapping.getSchema())
                            .withDefault(mapping.getDefaultValue());
                }
                return fields.endRecord();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Error processing key columns override for " + tableUpper, e);
        }

        // 2) Default spec per table (fixed lengths)
        LinkedHashMap<String, Integer> defaults = defaultKeyColumnSpecs.get(tableUpper);
        if (defaults != null && !defaults.isEmpty()) {
            for (Map.Entry<String, Integer> e : defaults.entrySet()) {
                String colName = e.getKey();
                int len = e.getValue() != null ? e.getValue() : 255;
                ColumnMetaData col = schemaTypeConverter.findColumnByName(tableMetaData, colName);
                Schema colSchema = Schema.create(Type.STRING);
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
                fields.name(colName).doc("").type(colSchema).withDefault("");
            }
            return fields.endRecord();
        }

        if (tableMetaData != null) {
            java.util.List<KeyFieldSpec> selected = new java.util.ArrayList<>();
            for (int i = 0; i < tableMetaData.getNumColumns(); i++) {
                ColumnMetaData col = tableMetaData.getColumnMetaData(i);
                if (col == null) {
                    continue;
                }
                if (!col.isKeyCol()) {
                    continue;
                }
                String colName = col.getColumnName();
                ColumnSchemaMapper.Mapping mapping = columnSchemaMapper.map(col);
                selected.add(new KeyFieldSpec(
                        safeGetKeyIndex(col), i, colName, mapping.getSchema()));
            }
            if (!selected.isEmpty()) {
                java.util.List<KeyFieldSpec> orderedFields = KeyFieldOrderResolver.order(
                        selected,
                        spec -> spec.keyIndex,
                        spec -> spec.tableIndex);
                java.util.List<String> orderedNames = new java.util.ArrayList<>();
                for (KeyFieldSpec spec : orderedFields) {
                    orderedNames.add(spec.columnName);
                    fields.name(spec.columnName).doc("").type(spec.schema)
                            .withDefault(schemaTypeConverter.getDefaultValue(spec.schema));
                }
                LOGGER.fine(() -> "Using GoldenGate key columns for " + tableUpper + ": " + orderedNames);
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
        return envelopeRecord;
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
    public Status metaDataChanged(DsEvent event, DsMetaData changedMetaData) {
        this.metaData = changedMetaData;
        schemaManager.clearCache();
        finalSchemaCache.clear();
        keySchemaCache.clear();
        registeredTopics.clear();
        LOGGER.info("GoldenGate metadata changed; schema caches were invalidated");
        return Status.OK;
    }

    @Override
    public void destroy() {
        if (kafkaProducer != null) {
            try {
                kafkaProducer.flush();
                kafkaProducer.close();
            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Error closing Kafka producer", ex);
            }
        }
        LOGGER.info(() -> "Handler stopped: " + reportStatus());
    }

    @Override
    public String reportStatus() {
        return "[KcopHandler] processed=" + operationCount.get();
    }

    private Level parseLogLevel(String configuredLevel) {
        String level = configuredLevel == null ? "INFO" : configuredLevel.trim().toUpperCase();
        switch (level) {
            case "DEBUG":
                return Level.FINE;
            case "WARN":
                return Level.WARNING;
            case "ERROR":
                return Level.SEVERE;
            default:
                return Level.parse(level);
        }
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

    private int safeGetKeyIndex(ColumnMetaData col) {
        if (col == null) {
            return -1;
        }
        try {
            return col.getKeyIndex();
        } catch (Exception ignore) {
            return -1;
        }
    }

    // Resolve topic from template; fallback keeps previous behavior if template is missing
    protected String resolveTopic(String template, String fullyQualifiedTableName) {
        String normalized = normalizeTopicTemplate(template);
        //System.out.println(">>> [KcopHandler] Resolving topic for table " + fullyQualifiedTableName + " using template: " + normalized);
        if (normalized == null || normalized.isEmpty()) {
            //System.out.println(">>> [KcopHandler] No topic template provided, using default topic naming: " + "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_"));
            return "cdc." + fullyQualifiedTableName.toLowerCase().replace(".", "_");
        }

        String fqn = fullyQualifiedTableName != null ? fullyQualifiedTableName : "";
        String table = fqn;
        String schema = "";
        String catalog = "";

        //System.out.println(">>> [KcopHandler] Parsing fully qualified table name: " + fqn);
        //aedt098
        if (fqn.contains(".")) {
            table = fqn.substring(fqn.lastIndexOf('.') + 1);
            //aedt098
            String prefix = fqn.substring(0, fqn.lastIndexOf('.'));
            //balp
            if (prefix.contains(".")) {
                schema = prefix.substring(prefix.lastIndexOf('.') + 1);
                //balp
                catalog = prefix.substring(0, prefix.lastIndexOf('.'));
            } else {
                //System.out.println(">>> [KcopHandler] No catalog part found, using prefix as schema: " + prefix);
                schema = prefix;
            }
        }
        //System.out.println(">>> [KcopHandler] Final parsed names - catalog: " + catalog + ", schema: " + schema + ", table: " + table);

        Map<String, String> vars = new HashMap<>();
        vars.put("fullyQualifiedTableName", fqn);
        vars.put("fullyQualifiedName", fqn);
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
            //System.out.println(">>> [KcopHandler] No substitution needed for template: " + template);
            return template;
        }

        Matcher m = PLACEHOLDER_PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String key = m.group(1) != null ? m.group(1).trim() : "";
            String replacement = vars.get(key);
            if (replacement == null) {
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group(0)));
            } else {
                m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        m.appendTail(sb);
        //System.out.println(">>> [KcopHandler] Substituted template: " + sb.toString());
        return sb.toString();
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
            LOGGER.log(Level.FINE, "Unable to resolve column name at index " + index, e);
        }
        return "COL_" + index;
    }
}
