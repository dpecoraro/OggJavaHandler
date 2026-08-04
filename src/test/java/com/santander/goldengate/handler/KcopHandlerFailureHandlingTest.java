package com.santander.goldengate.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import com.santander.goldengate.helpers.DecimalValueConverter;
import com.santander.goldengate.helpers.SchemaTypeConverter;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsColumn.BeforeAfter;
import oracle.goldengate.datasource.DsColumnAfterValue;
import oracle.goldengate.datasource.GGDataSource.Status;

class KcopHandlerFailureHandlingTest {

    @Test
    void mapsGoldenGateSqlNullToJavaNullWithoutConfusingLiteralNullText() {
        DsColumn sqlNull = columnWithAfterValue("NULL", true);
        DsColumn literalNull = columnWithAfterValue("NULL", false);

        assertNull(OperationDeliverySupport.extractColumnValue(sqlNull, BeforeAfter.AFTER));
        assertEquals("NULL", OperationDeliverySupport.extractColumnValue(literalNull, BeforeAfter.AFTER));
    }

    @Test
    void convertsNullableNumericUnionUsingItsNonNullBranch() {
        SchemaTypeConverter converter = new SchemaTypeConverter();
        DecimalValueConverter decimalConverter = new DecimalValueConverter();
        Schema numeric = Schema.create(Type.INT);
        numeric.addProp("logicalType", "DECIMAL");
        numeric.addProp("precision", 9);
        numeric.addProp("scale", 0);
        Schema nullableNumeric = Schema.createUnion(Arrays.asList(numeric, Schema.create(Type.NULL)));

        assertNull(converter.getDefaultValue(nullableNumeric));
        assertEquals(123, decimalConverter.convert(
                "123", converter.nonNullSchema(nullableNumeric), "NR_CPRO_PAGFOR"));
    }

    @Test
    void waitsForKafkaAcknowledgementAndPropagatesAsynchronousFailure() throws Exception {
        CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> failed = new CompletableFuture<>();
        SerializationException failure = new SerializationException("serialization failed");
        failed.completeExceptionally(failure);

        SerializationException thrown = assertThrows(
                SerializationException.class, () -> OperationDeliverySupport.await(failed));

        assertEquals(failure, thrown);
    }

    @Test
    void returnsAbendToStopReplicatWhenAnOperationFails() {
        assertEquals(Status.ABEND, OperationDeliverySupport.failureStatus());
    }

    private DsColumn columnWithAfterValue(String value, boolean sqlNull) {
        return sqlNull
                ? new DsColumnAfterValue(null, (byte[]) null)
                : new DsColumnAfterValue(value);
    }
}
