package com.ovhcloud.edc.dataplane.s3;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ovhcloud.edc.dataplane.s3.OVHCloudS3DataSource.Builder;
import com.ovhcloud.edc.dataplane.s3.OVHCloudS3DataSource.OVHCloudS3Part;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPIImpl;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure.Reason;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

class OVHCloudS3DataSourceTest {

  public static final long MEGABYTES = 1024L * 1024L;
  private S3ConnectorAPIImpl S3ConnectorAPI;
  private Monitor monitor;

  @BeforeEach
  void setUp() {
    monitor = mock(Monitor.class);
    S3ConnectorAPI = mock(S3ConnectorAPIImpl.class);
  }


  @Test
  void builderShouldThrowNullPointerExceptionWhenMinioClientIsNull() {
    OVHCloudS3DataSource.Builder builder = OVHCloudS3DataSource.Builder.newInstance(null);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenMonitorIsNull() {
    OVHCloudS3DataSource.Builder builder = OVHCloudS3DataSource.Builder.newInstance(
            S3ConnectorAPI)
        .monitor(null);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenBucketNameIsNull() {
    OVHCloudS3DataSource.Builder builder = OVHCloudS3DataSource.Builder.newInstance(
            S3ConnectorAPI)
        .monitor(monitor)
        .bucketName(null);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builderShouldReturnOVHCloudS3DataSourceWhenMandatoryDataAreSet() {
    OVHCloudS3DataSource dataSource = OVHCloudS3DataSource.Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectName("objectName")
        .objectPrefix("objectPrefix")
        .build();

    assertNotNull(dataSource);
  }

  @Test
  void openPartShouldFailsIfNoObjectFoundWithGivenPrefix() {
    // Given
    when(S3ConnectorAPI.listObjects("bucketName", "objectPrefix")).thenReturn(
        Collections.emptyList());

    OVHCloudS3DataSource OVHCloudS3DataSource = Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectPrefix("objectPrefix")
        .build();

    // When
    var result = OVHCloudS3DataSource.openPartStream();

    // Then
    assertTrue(result.failed());
    assertEquals(Reason.NOT_FOUND, result.reason());
  }

  @Test
  void openPartShouldSucceededIfObjectFoundWithGivenPrefix() {
    // Given
    when(S3ConnectorAPI.listObjects("bucketName", "objectPrefix")).thenReturn(
        List.of("objectPrefix/object1.txt", "objectPrefix/object2.txt"));

    when(S3ConnectorAPI.getObjectSize("bucketName", "objectPrefix/object1.txt")).thenReturn(
        MEGABYTES);
    when(S3ConnectorAPI.getObjectSize("bucketName", "objectPrefix/object2.txt")).thenReturn(
        10 * MEGABYTES);

    OVHCloudS3DataSource OVHCloudS3DataSource = Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectPrefix("objectPrefix")
        .build();

    // When
    var result = OVHCloudS3DataSource.openPartStream();

    // Then
    assertTrue(result.succeeded());
    result.getContent().forEach(part -> assertTrue(part.name().startsWith("objectPrefix")));
  }

  @Test
  void openPartShouldFailsIfListObjectThrowEdcException() {
    // Given
    when(S3ConnectorAPI.listObjects("bucketName", "objectName"))
        .thenThrow(EdcException.class);

    OVHCloudS3DataSource OVHCloudS3DataSource = Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectName("objectName")
        .build();

    // When
    var result = OVHCloudS3DataSource.openPartStream();

    // Then
    assertTrue(result.failed());
    assertEquals(Reason.GENERAL_ERROR, result.reason());
  }

  @Test
  void openPartShouldFailsIfNoObjectFound() {
    // Given
    when(S3ConnectorAPI.listObjects("bucketName", "objectName")).thenReturn(
        Collections.emptyList());
    OVHCloudS3DataSource OVHCloudS3DataSource = Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectName("objectName")
        .build();

    // When
    var result = OVHCloudS3DataSource.openPartStream();

    // Then
    assertTrue(result.failed());
    assertEquals(Reason.NOT_FOUND, result.reason());
  }

  @Test
  void openPartShouldSucceededIfObjectFoundWithoutPrefix() {
    // Given
    when(S3ConnectorAPI.listObjects("bucketName", "objectName")).thenReturn(
        List.of("objectName"));

    when(S3ConnectorAPI.getObjectSize("bucketName", "objectName")).thenReturn(
        1024L * 1024L);

    OVHCloudS3DataSource OVHCloudS3DataSource = Builder.newInstance(S3ConnectorAPI)
        .monitor(monitor)
        .bucketName("bucketName")
        .objectName("objectName")
        .build();

    // When
    var result = OVHCloudS3DataSource.openPartStream();

    // Then
    assertTrue(result.succeeded());
    result.getContent().forEach(part -> assertEquals("objectName", part.name()));
  }

}

class OVHCloudS3PartTest {

  private S3ConnectorAPIImpl S3ConnectorAPI;
  private Monitor monitor;
  private OVHCloudS3Part OVHCloudS3Part;

  @BeforeEach
  void setUp() {
    monitor = mock(Monitor.class);
    S3ConnectorAPI = mock(S3ConnectorAPIImpl.class);
    OVHCloudS3Part = new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", 0,
        1024L);

  }

  @Test
  void constructorShouldThrowNullPointerExceptionWhenMinioClientIsNull() {
    assertThrows(NullPointerException.class,
        () -> new OVHCloudS3Part(null, "bucketName", "objectName", 0, 0));
  }

  @Test
  void constructorShouldThrowNullPointerExceptionWhenBucketNameIsNull() {
    assertThrows(NullPointerException.class,
        () -> new OVHCloudS3Part(S3ConnectorAPI, null, "objectName", 0, 0));
  }

  @Test
  void constructorShouldThrowNullPointerExceptionWhenObjectNameIsNull() {
    assertThrows(NullPointerException.class,
        () -> new OVHCloudS3Part(S3ConnectorAPI, "bucketName", null, 0, 0));
  }

  @Test
  void constructorShouldThrowIllegalArgumentExceptionWhenStartIsNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", -1, 0));
  }

  @Test
  void constructorShouldThrowIllegalArgumentExceptionWhenEndIsNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", 0, -1));
  }

  @Test
  void constructorShouldThrowIllegalArgumentExceptionWhenEndIsLessThanStart() {
    assertThrows(IllegalArgumentException.class,
        () -> new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", 10, 5));
  }

  @Test
  void constructorShouldReturnOVHCloudS3PartWhenAllDataAreValid() {
    OVHCloudS3Part OVHCloudS3Part = new OVHCloudS3Part(S3ConnectorAPI,
        "bucketName", "objectName", 0, 10);

    assertNotNull(OVHCloudS3Part);
    assertEquals("objectName", OVHCloudS3Part.name());
  }

  @Test
  void openStreamShouldReturnStreamFailureWhenGetObjectThrowEdcException() {
    // Given
    when(S3ConnectorAPI.getObject("bucketName", "objectName", 0, 1024L)).thenThrow(
        EdcException.class);

    // When
    var result = OVHCloudS3Part.openStream();

    // Then
    assertNull(result);
  }

  @Test
  void openStreamShouldReturnValidInputStreamIfObjectSizeIsLowerThanChunkSize() {
    // Given
    when(S3ConnectorAPI.getObject("bucketName", "objectName"))
        .thenReturn(mock(ByteArrayInputStream.class));

    // When
    var result = OVHCloudS3Part.openStream();

    // Then
    assertNotNull(result);
    assertInstanceOf(ByteArrayInputStream.class, result);
    assertTrue(OVHCloudS3Part.isClosed());
  }

  @Test
  void openStreamShouldReturnValidInputStreamIfObjectSizeIsGreaterThanChunkSize() {
    // Given
    when(S3ConnectorAPI.getObject("bucketName", "objectName"))
        .thenReturn(mock(ByteArrayInputStream.class));

    var OVHCloudS3Part2 = new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", 0,
        1024L * 1024L * 10L);
    // When
    var result = OVHCloudS3Part2.openStream();

    // Then
    assertNotNull(result);
    assertInstanceOf(ByteArrayInputStream.class, result);
    assertTrue(OVHCloudS3Part2.isClosed());
  }

  @Test
  void openStreamWithOffsetAndSizeShouldReturnValidInputStreamWhenOffsetAndSizeAreOK() {
    // Given
    ByteArrayInputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.available()).thenReturn(1024);
    when(S3ConnectorAPI.getObject(
        "bucketName", "objectName", 0, 1024L))
        .thenReturn(inputStream);

    // When
    var result = OVHCloudS3Part.openStream(0, 1024L);

    // Then
    assertNotNull(result);
    assertInstanceOf(ByteArrayInputStream.class, result);
    assertTrue(OVHCloudS3Part.isClosed());
  }

  @Test
  void openStreamWithOffsetAndSizeShouldReturnValidInputStreamWhenOffsetIsLowerThanSize() {
    // Given
    ByteArrayInputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.available()).thenReturn(1024);
    when(S3ConnectorAPI.getObject(
        "bucketName", "objectName", 0, 1024L))
        .thenReturn(inputStream);

    // When
    OVHCloudS3Part = new OVHCloudS3Part(S3ConnectorAPI, "bucketName", "objectName", 0,
        2048L);
    var result = OVHCloudS3Part.openStream(0, 1024L);

    // Then
    assertNotNull(result);
    assertInstanceOf(ByteArrayInputStream.class, result);
    assertFalse(OVHCloudS3Part.isClosed());
  }

  @Test
  void openStreamWithOffsetAndSizeShouldThrowsIllegalStateExceptionWhenPartIsClosed() {
    // Given
    ByteArrayInputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.available()).thenReturn(1024);
    when(S3ConnectorAPI.getObject(
        "bucketName", "objectName", 0, 1024L))
        .thenReturn(inputStream);

    // When
    OVHCloudS3Part.openStream(0, 1024L);

    // Then
    assertThrows(IllegalStateException.class, () -> OVHCloudS3Part.openStream(0, 1024L));
  }

  @Test
  void openStreamWithOffsetAndSizeShouldThrowsEdcExceptionWhenAccessingInputStream() {
    // Given
    ByteArrayInputStream inputStream = mock(ByteArrayInputStream.class);
    when(inputStream.available()).thenThrow(IOException.class);
    when(S3ConnectorAPI.getObject(
        "bucketName", "objectName", 0, 1024L))
        .thenReturn(inputStream);

    // When
    // Then
    assertThrows(EdcException.class, () -> OVHCloudS3Part.openStream(0, 1024L));
  }

}