package com.ovhcloud.edc.dataplane.s3;


import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.validation.S3SourceDataAddressValidator;
import java.util.Objects;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link DataSourceFactory} implementation for OVHCloud S3 buckets.
 */
public class OVHCloudS3DataSourceFactory implements DataSourceFactory {

  private final S3ConnectorAPI s3Client;
  private final S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();
  private final Monitor monitor;

  public OVHCloudS3DataSourceFactory(S3ConnectorAPI s3Client, Monitor monitor) {
    Objects.requireNonNull(s3Client, "s3Client must not be null");
    Objects.requireNonNull(monitor, "monitor must not be null");
    this.s3Client = s3Client;
    this.monitor = monitor;
  }

  /**
   * Determines whether this factory can handle a {@link DataFlowStartMessage}.
   *
   * @param dataFlowStartMessage the {@link DataFlowStartMessage} to check.
   * @return {@code true} if this factory can handle the {@link DataFlowStartMessage}.
   */
  @Override
  public boolean canHandle(@NotNull DataFlowStartMessage dataFlowStartMessage) {
    monitor.debug("Checking if OVHCloudS3DataSourceFactory can handle the DataFlowStartMessage.");
    monitor.debug("DataFlowStartMessage source data address type: "
        + dataFlowStartMessage.getSourceDataAddress().getType());
    monitor.debug("OVHCloudS3DataSourceFactory can handle: " + OVHCloudBucketSchema.TYPE);

    return OVHCloudBucketSchema.TYPE.equals(dataFlowStartMessage.getSourceDataAddress().getType());
  }

  /**
   * Creates a new {@link DataSource} for a {@link DataFlowStartMessage}.
   *
   * @param dataFlowStartMessage the {@link DataFlowStartMessage} to create a {@link DataSource}
   *                             for.
   * @return the new {@link DataSource}.
   */
  @Override
  public DataSource createSource(DataFlowStartMessage dataFlowStartMessage) {
    monitor.debug("Creating a new OVHCloudDataSource for the DataFlowStartMessage.");
    monitor.debug("DataFlowStartMessage source data address: "
        + dataFlowStartMessage.getSourceDataAddress());
    monitor.debug("DataFlowStartMessage source data address properties: "
        + dataFlowStartMessage.getSourceDataAddress().getProperties());

    var validationResult = validateRequest(dataFlowStartMessage);

    if (validationResult.failed()) {
      monitor.severe("Validation failed: " + validationResult.getFailureMessages());
      throw new EdcException(String.join("; ", validationResult.getFailureMessages()));
    }

    var source = dataFlowStartMessage.getSourceDataAddress();

    return OVHCloudS3DataSource.Builder.newInstance(this.s3Client)
        .monitor(this.monitor)
        .bucketName(source.getStringProperty(OVHCloudBucketSchema.BUCKET_NAME))
        .objectName(source.getStringProperty(OVHCloudBucketSchema.OBJECT_NAME))
        .objectPrefix(source.getStringProperty(OVHCloudBucketSchema.OBJECT_PREFIX))
        .build();
  }

  /**
   * Validates a {@link DataFlowStartMessage}.
   *
   * @param dataFlowStartMessage the {@link DataFlowStartMessage} to validate.
   * @return the validation result.
   */
  @Override
  public @NotNull Result<Void> validateRequest(DataFlowStartMessage dataFlowStartMessage) {
    return validator.validate(dataFlowStartMessage.getSourceDataAddress()).flatMap(
        ValidationResult::toResult);
  }
}
