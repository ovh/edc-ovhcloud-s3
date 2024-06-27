package com.ovhcloud.edc.dataplane.s3;

import java.util.List;
import java.util.Objects;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.jetbrains.annotations.NotNull;

public class OVHCloudS3DataSink extends ParallelSink {

  private OVHCloudS3DataSink() {
  }

  private S3ConnectorAPI S3ConnectorAPI;
  private String bucketName;
  private String objectName;
  private String path;

  @Override
  protected StreamResult<Object> transferParts(List<Part> parts) {
    for (Part part : parts) {
      String destination = determineDestinationName(part);
      try {
        uploadPartToOVHCloudS3(part, destination);
      } catch (Exception e) {
        return handleUploadException(part, e);
      }
    }

    return StreamResult.success();
  }

  private @NotNull StreamResult<Object> handleUploadException(Part part, Exception e) {
    var message = String.format("Error uploading part %s to OVHCloudS3 bucket %s", part.name(),
        bucketName);
    monitor.severe(message, e);
    return StreamResult.error(message);
  }

  private void uploadPartToOVHCloudS3(Part part, String destination) {
    S3ConnectorAPI.putObject(bucketName, destination, part.openStream());
  }

  String determineDestinationName(Part part) {
    String destination = "";

    if (path != null && objectName != null) {
      // Add a trailing slash to path, if path is not empty and there is no trailing
      // slash
      destination = !path.isEmpty() && !path.endsWith("/") ? path + "/" + objectName : path + objectName;
    } else if (path == null && objectName != null) {
      destination = objectName;
    } else if (path != null) {
      // Add a trailing slash to path, if path is not empty and there is no trailing
      // slash
      destination = !path.isEmpty() && !path.endsWith("/") ? path + "/" + part.name() : path + part.name();
    } else {
      destination = part.name();
    }

    return destination;
  }

  public static class Builder extends ParallelSink.Builder<Builder, OVHCloudS3DataSink> {

    private Builder() {
      super(new OVHCloudS3DataSink());
    }

    public static Builder newInstance() {
      return new Builder();
    }

    public Builder s3ConnectorAPI(S3ConnectorAPI S3ConnectorAPI) {
      sink.S3ConnectorAPI = S3ConnectorAPI;
      return this;
    }

    public Builder bucketName(String bucketName) {
      sink.bucketName = bucketName;
      return this;
    }

    public Builder objectName(String objectName) {
      sink.objectName = objectName;
      return this;
    }

    public Builder path(String path) {
      sink.path = path;
      return this;
    }

    @Override
    protected void validate() {
      Objects.requireNonNull(sink.S3ConnectorAPI, "OVHCloudS3 client is required");
      Objects.requireNonNull(sink.bucketName, "Bucket Name is required");
    }
  }
}
