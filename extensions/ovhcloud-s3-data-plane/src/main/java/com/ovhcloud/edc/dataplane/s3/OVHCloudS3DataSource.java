package com.ovhcloud.edc.dataplane.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure.Reason;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link DataSource} implementation for OVHCloudS3 S3 buckets.
 */
public class OVHCloudS3DataSource implements DataSource {

  private final S3ConnectorAPI client;
  private final Monitor monitor;
  private final String bucketName;
  private final Optional<String> objectName;
  private final Optional<String> objectPrefix;

  private OVHCloudS3DataSource(Builder builder) {
    this.client = builder.client;
    this.monitor = builder.monitor;
    this.bucketName = builder.bucketName;
    this.objectName = builder.objectName;
    this.objectPrefix = builder.objectPrefix;
  }

  /**
   * Opens a stream of parts for the data source.
   *
   * @return a stream of parts for the data source.
   */
  @Override
  public StreamResult<Stream<Part>> openPartStream() {
    monitor.debug(
        String.format("Opening part stream for OVHCloudS3 data source %s/%s", bucketName, objectPrefix));

    try {
      var objects = client.listObjects(bucketName, objectPrefix.orElse(objectName.orElse(null)));
      if (objects.isEmpty()) {
        return StreamResult.failure(new StreamFailure(
            List.of(String.format(
                "Error listing OVHCloudS3 objects in the bucket %s: Object not found with prefix %s",
                bucketName, objectPrefix)),
            Reason.NOT_FOUND));
      }

      return StreamResult.success(
          objects.stream().map(
              objectName -> {
                long objectSize = client.getObjectSize(bucketName, objectName);
                return new OVHCloudS3Part(client, bucketName, objectName, 0, objectSize);
              }
          )
      );

    } catch (EdcException e) {
      return StreamResult.failure(new StreamFailure(
          List.of(String.format(
              "Error opening part stream for OVHCloudS3 data source %s/%s: %s",
              bucketName, objectPrefix, e.getMessage())),
          Reason.GENERAL_ERROR));
    }

  }

  /**
   * Closes the data source.
   *
   * @throws Exception if an error occurs while closing the data source.
   */
  @Override
  public void close() throws Exception {

  }

  public static final class OVHCloudS3Part implements Part {

    private static final long MEGA_BYTE = 1024L * 1024L;
    private static final long DEFAULT_CHUNK_SIZE = 5L * MEGA_BYTE; // 5MB
    private static final String PART_CLOSED_ERROR = "Part is already closed";
    private static final String READ_ERROR = "Error when reading %s from bucket %s";

    private final S3ConnectorAPI client;
    private final String bucketName;
    private final String objectName;
    private final long size;

    private boolean closed = false;
    private long offset;

    OVHCloudS3Part(S3ConnectorAPI client, String bucketName, String objectName, long offset,
        long size) {

      validateClient(client);
      validateBucketName(bucketName);
      validateObjectName(objectName);
      validateOffset(offset);
      validateSize(offset, size);

      this.client = client;
      this.bucketName = bucketName;
      this.objectName = objectName;
      this.offset = offset;
      this.size = size;
    }

    private static void validateSize(long offset, long size) {
      if (size <= 0) {
        throw new IllegalArgumentException("Size must be greater than or equal to 0");
      }

      if (size < offset) {
        throw new IllegalArgumentException("Size must be greater than or equal to offset");
      }
    }

    private static void validateOffset(long offset) {
      if (offset < 0) {
        throw new IllegalArgumentException("Offset must be greater than or equal to 0");
      }
    }

    private static void validateObjectName(String objectName) {
      Objects.requireNonNull(objectName, "objectName must not be null");
    }

    private static void validateBucketName(String bucketName) {
      Objects.requireNonNull(bucketName, "bucketName must not be null");
    }

    private void validateClient(S3ConnectorAPI client) {
      Objects.requireNonNull(client, "S3ConnectorAPI must not be null");
    }

    /**
     * Returns the name of the part.
     *
     * @return the name of the part.
     */
    @Override
    public String name() {
      return objectName;
    }

    /**
     * Returns the size of the part.
     *
     * @return the size of the part.
     */
    @Override
    public long size() {
      return size;
    }

    public boolean isClosed() {
      return closed;
    }

    public double offset() {
      return offset;
    }

    @Override
    public InputStream openStream() {
      checkIfPartIsClosed();
      var inputStream = client.getObject(bucketName, objectName);
      closed = true;

      return inputStream;
    }

    public InputStream openStream(long start, long end) {
      checkIfPartIsClosed();

      InputStream inputStream = client.getObject(bucketName, objectName, start, end);

      updateOffset(inputStream);
      closePartIfOffsetExceedsSize();

      return inputStream;
    }

    private void checkIfPartIsClosed() {
      if (closed) {
        throw new IllegalStateException(PART_CLOSED_ERROR);
      }
    }

    private void updateOffset(InputStream inputStream) {
      try {
        offset += inputStream.available();
      } catch (IOException e) {
        throw new EdcException(String.format(READ_ERROR, objectName, bucketName), e);
      }
    }

    private void closePartIfOffsetExceedsSize() {
      if (offset >= size) {
        closed = true;
      }
    }
  }

  /**
   * A builder for {@link OVHCloudS3DataSource}.
   */
  public static class Builder {

    private final S3ConnectorAPI client;
    private Monitor monitor;
    private String bucketName;
    private Optional<String> objectName = Optional.empty();
    private Optional<String> objectPrefix = Optional.empty();

    private Builder(S3ConnectorAPI client) {
      this.client = client;
    }

    /**
     * Creates a new instance of {@link Builder}.
     *
     * @param client the OVHCloudS3 client.
     * @return a new instance of {@link Builder}.
     */
    @Contract(value = "_ -> new", pure = true)
    public static @NotNull Builder newInstance(S3ConnectorAPI client) {
      return new Builder(client);
    }

    /**
     * Sets the monitor.
     *
     * @param monitor the monitor.
     * @return the builder.
     */
    public Builder monitor(Monitor monitor) {
      this.monitor = monitor;
      return this;
    }

    /**
     * Sets the bucket name.
     *
     * @param bucketName the bucket name.
     * @return the builder.
     */
    public Builder bucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    /**
     * Sets the object name.
     *
     * @param objectName the object name.
     * @return the builder.
     */
    public Builder objectName(String objectName) {
      this.objectName = Optional.of(objectName);
      return this;
    }

    /**
     * Sets the object prefix.
     *
     * @param objectPrefix the object prefix.
     * @return the builder.
     */
    public Builder objectPrefix(String objectPrefix) {
      if (objectPrefix == null) {
        this.objectPrefix = Optional.empty();
        return this;
      }

      this.objectPrefix = Optional.of(objectPrefix);
      return this;
    }

    /**
     * Builds the {@link OVHCloudS3DataSource}.
     *
     * @return the {@link OVHCloudS3DataSource}.
     */
    public OVHCloudS3DataSource build() {
      Objects.requireNonNull(client, "OVHCloudS3ConnectorAPIClient must not be null");
      Objects.requireNonNull(monitor, "monitor must not be null");
      Objects.requireNonNull(bucketName, "bucketName must not be null");

      return new OVHCloudS3DataSource(this);
    }
  }
}
