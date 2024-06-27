package com.ovhcloud.edc.dataplane.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPIImpl;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudRegions;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OVHCloudS3DataSourceFactoryTest {

  private S3ConnectorAPIImpl OVHCloudConnectorAPI;
  private Monitor monitor;

  @BeforeEach
  void setUp() {
    monitor = mock(Monitor.class);
    OVHCloudConnectorAPI = mock(S3ConnectorAPIImpl.class);
  }

  @Test
  void constructorShouldThrowNullPointerExceptionWhenOVHCloudClientIsNull() {
    assertThrows(NullPointerException.class, () -> new OVHCloudS3DataSourceFactory(null, monitor));
  }

  @Test
  void constructorShouldThrowNullPointerExceptionWhenMonitorIsNull() {
    assertThrows(NullPointerException.class,
        () -> new OVHCloudS3DataSourceFactory(OVHCloudConnectorAPI, null));
  }

  @Test
  void constructorShouldNotThrowExceptionWhenOVHCloudClientAndMonitorAreNotNull() {
    assertDoesNotThrow(() -> new OVHCloudS3DataSourceFactory(OVHCloudConnectorAPI, monitor));
  }

  @Test
  void canHandleShouldReturnTrueWhenDataFlowStartMessageSourceDataAddressTypeIsOVHCloudBucketSchemaType() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

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
    boolean result = OVHCloudS3DataSourceFactory.canHandle(dataFlowStartMessage);

    // Then
    assertTrue(result);
  }

  @Test
  void canHandleShouldReturnFalseWhenDataFlowStartMessageSourceDataAddressTypeIsNotOVHCloudBucketSchemaType() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

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
    boolean result = OVHCloudS3DataSourceFactory.canHandle(dataFlowStartMessage);

    // Then
    assertFalse(result);
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageSourceDataAddressIsValid() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressPropertyBucketNameIsMissing() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSucceededResultWhenDataFlowStartMessageSourceDataAddressPropertyRegionIsMissing() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

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
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressPropertyObjectNameIsMissing() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressPropertyObjectPrefixIsMissing() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressIsInvalid() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

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
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(2, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressPropertyBucketNameIsEmpty() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnFailureResultWhenDataFlowStartMessageSourceDataAddressPropertyObjectNameIsEmpty() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_NAME, "")
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.failed());
    assertEquals(1, result.getFailureMessages().size());
  }

  @Test
  void validateRequestShouldReturnSuccessResultWhenDataFlowStartMessageSourceDataAddressPropertyObjectPrefixIsEmpty() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    var result = OVHCloudS3DataSourceFactory.validateRequest(dataFlowStartMessage);

    // Then
    assertTrue(result.succeeded());
    assertEquals(0, result.getFailureMessages().size());
  }

  @Test
  void createSourceShouldThrowExceptionWhenSourceDataAddressIsInvalid() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

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
        () -> OVHCloudS3DataSourceFactory.createSource(dataFlowStartMessage));
  }

  @Test
  void createSourceShouldReturnNonNullDataSourceWhenSourceDataAddressIsValid() {
    // Given
    OVHCloudS3DataSourceFactory OVHCloudS3DataSourceFactory = new OVHCloudS3DataSourceFactory(
        OVHCloudConnectorAPI, monitor);

    DataFlowStartMessage dataFlowStartMessage = DataFlowStartMessage.Builder.newInstance()
        .processId("processId")
        .sourceDataAddress(DataAddress.Builder.newInstance()
            .type(OVHCloudBucketSchema.TYPE)
            .property(OVHCloudBucketSchema.BUCKET_NAME, "bucketName")
            .property(OVHCloudBucketSchema.OBJECT_NAME, "objectName")
            .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
            .property(OVHCloudBucketSchema.OBJECT_PREFIX, "objectPrefix")
            .build())
        .destinationDataAddress(DataAddress.Builder.newInstance()
            .type("destinationType")
            .build())
        .build();

    // When
    DataSource result = OVHCloudS3DataSourceFactory.createSource(dataFlowStartMessage);

    // Then
    assertNotNull(result);
  }

}
