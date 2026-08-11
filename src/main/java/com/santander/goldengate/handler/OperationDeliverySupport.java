package com.santander.goldengate.handler;

import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsColumn.BeforeAfter;
import oracle.goldengate.datasource.GGDataSource.Status;

final class OperationDeliverySupport {

    private OperationDeliverySupport() {
    }

    static Object extractColumnValue(DsColumn column, BeforeAfter image) {
        DsColumn imageColumn = column.getChild(image);
        if (imageColumn == null) {
            imageColumn = image == BeforeAfter.AFTER ? column.getAfter() : column.getBefore();
        }
        if (imageColumn != null && imageColumn.isValueNull()) {
            return null;
        }
        Object value = image == BeforeAfter.AFTER
                ? column.getAfterValue()
                : column.getBeforeValue();
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return value;
    }

    static <T> T await(Future<T> delivery) throws Exception {
        try {
            return delivery.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw ex;
        }
    }

    static Object resolveSqlNull(Schema.Field field) {
        if (allowsNull(field.schema())) {
            return null;
        }
        if (field.hasDefaultValue()) {
            return GenericData.get().getDefaultValue(field);
        }
        throw new IllegalArgumentException(
                "SQL NULL received for non-nullable field without default " + field.name());
    }

    private static boolean allowsNull(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream()
                        .anyMatch(candidate -> candidate.getType() == Schema.Type.NULL);
    }

    static Status failureStatus() {
        return Status.ABEND;
    }
}
