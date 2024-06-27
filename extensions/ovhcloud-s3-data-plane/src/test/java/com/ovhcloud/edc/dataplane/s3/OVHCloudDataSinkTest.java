package com.ovhcloud.edc.dataplane.s3;

import com.ovhcloud.edc.dataplane.s3.OVHCloudS3DataSource.OVHCloudS3Part;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPIImpl;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OVHCloudDataSinkTest {

  private S3ConnectorAPIImpl s3ConnectorAPI;
  private Monitor monitor;
  private ExecutorService executorService;
  private OVHCloudS3DataSource.OVHCloudS3Part part;

  @BeforeEach
  void setUp() {
    monitor = mock(Monitor.class);
    s3ConnectorAPI = mock(S3ConnectorAPIImpl.class);
    executorService = mock(ExecutorService.class);

    part = mock(OVHCloudS3DataSource.OVHCloudS3Part.class);
    when(part.name()).thenReturn("partName");
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenMinioConnectorClientIsNull() {
    OVHCloudS3DataSink.Builder builder = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .bucketName("bucketName")
        .executorService(executorService)

        .bucketName(null);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builderShouldThrowNullPointerExceptionWhenBucketNameIsNull() {
    OVHCloudS3DataSink.Builder builder = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .s3ConnectorAPI(s3ConnectorAPI)
        .requestId("requestId")
        .executorService(executorService)

        .bucketName(null);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builderShouldReturnOVHCloudS3DataSinkWhenMandatoryDataAreSet() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .objectName("objectName")
        .build();

    assertNotNull(dataSink);
  }

  @Test
  void determineDestinationNameShouldReturnNonEmptyStringWhenPathAndObjectNameNonNull() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .objectName("objectName")
        .path("path/")
        .build();

    String destination = dataSink.determineDestinationName(mock(OVHCloudS3Part.class));

    assertNotNull(destination);
    assertNotEquals("", destination);
    assertEquals("path/objectName", destination);
  }

  @Test
  void determineDestinationNameShouldReturnValidFullPathWhenPathSetWithoutTrailingSlash() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .objectName("objectName")
        .path("path") // <-- notice missing trailing slash here
        .build();

    String destination = dataSink.determineDestinationName(mock(OVHCloudS3Part.class));

    assertNotNull(destination);
    assertNotEquals("", destination);
    assertEquals("path/objectName", destination);
  }

  @Test
  void determineDestinationShouldReturnNonEmptyStringWhenPathNullAndObjectNameNonNull() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .objectName("objectName")
        .build();

    String destination = dataSink.determineDestinationName(mock(OVHCloudS3Part.class));

    assertNotNull(destination);
    assertNotEquals("", destination);
    assertEquals("objectName", destination);
  }

  @Test
  void determineDestinationShouldReturnNonEmptyStringWhenPathNonNullAndObjectNameNull() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .path("path/")
        .build();

    var part = mock(OVHCloudS3Part.class);
    when(part.name()).thenReturn("partName");

    String destination = dataSink.determineDestinationName(part);

    assertNotNull(destination);
    assertNotEquals("", destination);
    assertEquals("path/partName", destination);
  }

  @Test
  void determineDestinationShouldReturnNonEmptyStringWhenPathNullAndObjectNameNull() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .build();

    var part = mock(OVHCloudS3Part.class);
    when(part.name()).thenReturn("partName");

    String destination = dataSink.determineDestinationName(part);

    assertNotNull(destination);
    assertNotEquals("", destination);
    assertEquals("partName", destination);
  }

  @Test
  void determineDestinationShouldReturnPartNameWhenPathAndObjectNameAreNull() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .build();

    String destination = dataSink.determineDestinationName(part);
    assertEquals("partName", destination);
  }

  @Test
  void determineDestinationShouldReturnPartNameWhenPathAndObjectNameAreEmpty() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .objectName("")
        .path("")
        .build();
    String destination = dataSink.determineDestinationName(part);
    assertEquals("", destination);
  }

  @Test
  void determineDestinationNameShouldHandleSpecialCharactersInPathAndObjectName() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .path("path/with/special/chars#%$&/")
        .objectName("objectName#%$&")
        .build();

    String destination = dataSink.determineDestinationName(part);
    assertEquals("path/with/special/chars#%$&/objectName#%$&", destination);
  }

  @Test
  void determineDestinationNameShouldHandleSpecialCharactersInPartName() {
    OVHCloudS3DataSink dataSink = OVHCloudS3DataSink.Builder.newInstance()
        .monitor(monitor)
        .requestId("requestId")
        .executorService(executorService)
        .s3ConnectorAPI(s3ConnectorAPI)
        .bucketName("bucketName")
        .build();
    when(part.name()).thenReturn("partName#%$&");
    String destination = dataSink.determineDestinationName(part);
    assertEquals("partName#%$&", destination);
  }
}
