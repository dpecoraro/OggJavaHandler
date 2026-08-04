package com.santander.goldengate.handler;

import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    static Status failureStatus() {
        return Status.ABEND;
    }
}
