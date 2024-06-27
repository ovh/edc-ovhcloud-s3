package com.ovhcloud.edc.dataplane.s3;

import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.validation.S3DestinationDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.jetbrains.annotations.NotNull;
import java.util.concurrent.ExecutorService;

/**
 * A {@link DataSinkFactory} implementation for S3 S3 buckets.
 */
public class OVHCloudS3DataSinkFactory implements DataSinkFactory {

  private final S3ConnectorAPI s3ConnectorAPI;
  private final Monitor monitor;
  private final S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();
  private final ExecutorService executorService;

  public OVHCloudS3DataSinkFactory(S3ConnectorAPI s3ConnectorAPI, Monitor monitor, ExecutorService executorService) {
    this.s3ConnectorAPI = s3ConnectorAPI;
    this.monitor = monitor;
    this.executorService = executorService;
  }

  /**
   * Determines whether this factory can handle a {@link DataFlowStartMessage}.
   *
   * @param dataFlowStartMessage the {@link DataFlowStartMessage} to check.
   * @return {@code true} if this factory can handle the {@link DataFlowStartMessage}.
   */
  @Override
  public boolean canHandle(DataFlowStartMessage dataFlowStartMessage) {
    return OVHCloudBucketSchema.TYPE.equals(
        dataFlowStartMessage.getDestinationDataAddress().getType());
  }

  /**
   * @param dataFlowStartMessage
   * @return
   */
  @Override
  public DataSink createSink(DataFlowStartMessage dataFlowStartMessage) {
    var validationResult = validateRequest(dataFlowStartMessage);
    if (validationResult.failed()) {
      throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
    }

    var destination = dataFlowStartMessage.getDestinationDataAddress();
    var builder = OVHCloudS3DataSink.Builder.newInstance()
        .requestId(dataFlowStartMessage.getId())
        .monitor(monitor)
        .s3ConnectorAPI(s3ConnectorAPI)
        .executorService(executorService)
        .bucketName(destination.getStringProperty(OVHCloudBucketSchema.BUCKET_NAME));

    if (destination.hasProperty(OVHCloudBucketSchema.PATH)) {
      builder.path(destination.getStringProperty(OVHCloudBucketSchema.PATH));
    }

    if (destination.hasProperty(OVHCloudBucketSchema.OBJECT_NAME)) {
      builder.objectName(destination.getStringProperty(OVHCloudBucketSchema.OBJECT_NAME));
    }

    return builder
        .build();
  }

  /**
   * Validates the request.
   *
   * @param dataFlowStartMessage the {@link DataFlowStartMessage} to validate.
   * @return a {@link Result} indicating whether the request is valid.
   */
  @Override
  public @NotNull Result<Void> validateRequest(DataFlowStartMessage dataFlowStartMessage) {
    return validator.validate(dataFlowStartMessage.getDestinationDataAddress()).flatMap(
        ValidationResult::toResult);
  }
}
