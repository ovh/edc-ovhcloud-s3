package com.ovhcloud.edc.dataplane.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutorService;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPIImpl;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudRegions;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OVHCloudDataSinkFactoryTest {

  private S3ConnectorAPIImpl S3ConnectorAPI;
  private Monitor monitor;
  private ExecutorService executorService;

  @BeforeEach
  void setUp() {
    monitor = mock(Monitor.class);
    S3ConnectorAPI = mock(S3ConnectorAPIImpl.class);
    executorService = mock(ExecutorService.class);
  }

  @Test
  void canHandleShouldReturnTrueWhenDataFlowStartMessageDestinationDataAddressTypeIsOVHCloudBucketSchemaType() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type("sourceType")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .build())
        .build();

    // When
    boolean result = factory.canHandle(dataFlowStartMessage);

    // Then
    assertTrue(result);
  }

  @Test
  void canHandleShouldReturnFalseWhenDataFlowStartMessageDestinationDataAddressTypeIsNotOVHCloudBucketSchemaType() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type("sourceType")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    boolean result = factory.canHandle(dataFlowStartMessage);

    // Then
    assertFalse(result);
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageDestinationDataAddressIsValid() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "destinationBucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageDestinationDataAddressPropertyBucketNameIsMissing() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageDestinationDataAddressPropertyObjectNameIsMissing() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "destinationBucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageDestinationDataAddressPropertyObjectPrefixIsMissing() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "destinationBucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageDestinationDataAddressIsInvalid() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(2, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageDestinationDataAddressPropertyBucketNameIsEmpty() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .property(OVHCloudBucketSchema.BUCKET_NAME, "")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageDestinationDataAddressPropertyRegionIsMissing() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "destinationBucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageDestinationDataAddressPropertyRegionIsEmpty() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.REGION, "")
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageDestinationDataAddressPropertyObjectNameIsEmpty() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageDestinationDataAddressPropertyObjectPrefixIsEmpty() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    var result = factory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void createSinkShouldThrowExceptionWhenDestinationDataAddressIsInvalid() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type("sourceType")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    // Then
    assertThrows(EdcException.class,
        () -> factory.createSink(dataFlowStartMessage));
  }

    @Test
  void createSourceShouldReturnNonNullDataSinkWheDestinationDataAddressIsValid() {
    // Given
    OVHCloudS3DataSinkFactory factory = new OVHCloudS3DataSinkFactory(S3ConnectorAPI,
        monitor, executorService);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "destinationBucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .build())
        .build();

    // When
    DataSink result = factory.createSink(dataFlowStartMessage);

    // Then
    assertNotNull(result);
  }

}
