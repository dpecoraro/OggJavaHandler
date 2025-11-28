package com.santander.goldengate.handler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;

/**
 * Lightweight Schema Registry client (basic POST register).
 */
public class SchemaRegistryClient {

    private final Set<String> registryUrls = new LinkedHashSet<>();
    private final Set<String> registeredSubjects = new LinkedHashSet<>();

    public void init(Properties props) {
        String valueUrls = props.getProperty("value.converter.schema.registry.url");
        String keyUrls = props.getProperty("key.converter.schema.registry.url");
        String raw = (valueUrls != null && !valueUrls.isEmpty()) ? valueUrls : keyUrls;
        if (raw != null && !raw.isEmpty()) {
            for (String u : raw.split(",")) {
                String t = u.trim();
                if (!t.isEmpty()) registryUrls.add(t);
            }
        }
        System.out.println(">>> [SchemaRegistryClient] URLs=" + registryUrls);
    }

    public void registerIfNeeded(String subject, Schema schema) {
        if (schema == null || subject == null || subject.isEmpty() || registryUrls.isEmpty()) return;
        if (registeredSubjects.contains(subject)) return;
        try {
            int id = register(subject, schema);
            if (id > 0) {
                registeredSubjects.add(subject);
                System.out.println(">>> [SchemaRegistryClient] Registered subject=" + subject + " id=" + id);
            } else {
                System.out.println(">>> [SchemaRegistryClient] No id returned subject=" + subject);
            }
        } catch (Exception e) {
            System.err.println("[SchemaRegistryClient] Failed subject=" + subject + " msg=" + e.getMessage());
        }
    }

    private int register(String subject, Schema schema) throws Exception {
        Exception last = null;
        for (String base : registryUrls) {
            try {
                int id = post(base, subject, schema);
                if (id > 0) return id;
            } catch (Exception e) {
                last = e;
                System.err.println("[SchemaRegistryClient] POST failed url=" + base + " msg=" + e.getMessage());
            }
        }
        if (last != null) throw last;
        return 0;
    }

    private int post(String baseUrl, String subject, Schema schema) throws Exception {
        String endpoint = (baseUrl.endsWith("/") ? baseUrl : baseUrl + "/")
                + "subjects/" + URLEncoder.encode(subject, "UTF-8") + "/versions";
        String schemaJson = schema.toString();
        String escaped = schemaJson.replace("\\", "\\\\").replace("\"", "\\\"");
        String payload = "{\"schema\":\"" + escaped + "\"}";

        HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
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
        StringBuilder body = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream(),
                        StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) body.append(line);
        }
        if (code >= 200 && code < 300) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"id\"\\s*:\\s*(\\d+)").matcher(body);
            if (m.find()) return Integer.parseInt(m.group(1));
            return 0;
        }
        throw new RuntimeException("Registry error (" + code + "): " + body);
    }
}
