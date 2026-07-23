package com.santander.goldengate.handler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loads the DB2 schemas embedded in the production comparison report and makes
 * them available by table name. Table names are unique in the supplied report.
 */
public final class Db2SchemaContractCatalog {
    static final String RESOURCE = "/contracts/db2-schema-contracts.json";

    private final Map<String, Schema> keySchemas;
    private final Map<String, Schema> valueSchemas;

    private Db2SchemaContractCatalog(
            Map<String, Schema> keySchemas,
            Map<String, Schema> valueSchemas) {
        this.keySchemas = keySchemas;
        this.valueSchemas = valueSchemas;
    }

    public static Db2SchemaContractCatalog loadBundled() {
        try (InputStream input = Db2SchemaContractCatalog.class.getResourceAsStream(RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("Bundled DB2 schema contract was not found: " + RESOURCE);
            }
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = new ObjectMapper().readTree(json);
            return new Db2SchemaContractCatalog(
                    parseSchemas(root.path("key")),
                    parseSchemas(root.path("value")));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to load bundled DB2 schema contract", ex);
        }
    }

    public Schema keySchema(String tableName) {
        return keySchemas.get(shortTableName(tableName));
    }

    public Schema valueSchema(String tableName) {
        return valueSchemas.get(shortTableName(tableName));
    }

    public int keySchemaCount() {
        return keySchemas.size();
    }

    public int valueSchemaCount() {
        return valueSchemas.size();
    }

    private static Map<String, Schema> parseSchemas(JsonNode schemasNode) {
        Map<String, Schema> schemas = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = schemasNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String subject = entry.getKey();
            String tableName = subject.substring(subject.lastIndexOf('.') + 1)
                    .toLowerCase(Locale.ROOT);
            Schema schema = new Schema.Parser().parse(entry.getValue().toString());
            Schema previous = schemas.put(tableName, schema);
            if (previous != null) {
                throw new IllegalStateException("Duplicate DB2 table contract: " + tableName);
            }
        }
        return schemas;
    }

    private static String shortTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return "";
        }
        int separator = tableName.lastIndexOf('.');
        String shortName = separator >= 0 ? tableName.substring(separator + 1) : tableName;
        return shortName.toLowerCase(Locale.ROOT);
    }
}
