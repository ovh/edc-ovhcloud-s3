package com.ovhcloud.edc.provision.s3;

import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudRegions;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.jetbrains.annotations.Nullable;
import java.util.Objects;
import java.util.Optional;

import static java.util.UUID.randomUUID;

/**
 * A {@link ConsumerResourceDefinitionGenerator} implementation for OVHCloud S3 buckets.
 */
public class OVHCloudS3ConsumerResourceDefinitionGenerator implements
    ConsumerResourceDefinitionGenerator {

  /**
   * Generates a {@link ResourceDefinition} for the given {@link TransferProcess} and
   * {@link Policy}.
   *
   * @param transferProcess The {@link TransferProcess} for which to generate the
   *                        {@link ResourceDefinition}.
   * @param policy          The {@link Policy} for which to generate the
   *                        {@link ResourceDefinition}.
   * @return The generated {@link ResourceDefinition}.
   */
  @Override
  public @Nullable ResourceDefinition generate(TransferProcess transferProcess, Policy policy) {
    Objects.requireNonNull(transferProcess, "transferProcess must always be provided");
    Objects.requireNonNull(policy, "policy must always be provided");

    var destination = transferProcess.getDataDestination();
    var id = randomUUID().toString();
    var bucketName = destination.getStringProperty(OVHCloudBucketSchema.BUCKET_NAME);
    var region = destination.getStringProperty(OVHCloudBucketSchema.REGION);

    if (region == null) {
      region = OVHCloudRegions.DEFAULT_REGION;
    }

    var builder = OVHCloudS3ResourceDefinition.Builder.newInstance()
        .id(id)
        .bucketName(bucketName)
        .region(region);

    if (destination.hasProperty(OVHCloudBucketSchema.OBJECT_NAME)) {
      builder.objectName(
          Optional.ofNullable(destination.getStringProperty(OVHCloudBucketSchema.OBJECT_NAME)));
    } else {
      builder.objectName(Optional.empty());
    }

    if (destination.hasProperty(OVHCloudBucketSchema.PATH)) {
      builder.path(Optional.ofNullable(destination.getStringProperty(OVHCloudBucketSchema.PATH)));
    } else {
      builder.path(Optional.empty());
    }

    return builder.build();
  }


  /**
   * Determines whether this generator can generate a {@link ResourceDefinition} for the given
   * {@link TransferProcess} and {@link Policy}.
   *
   * @param transferProcess The {@link TransferProcess} for which to determine whether a
   *                        {@link ResourceDefinition} can be generated. Must not be {@code null}.
   * @param policy          The {@link Policy} for which to determine whether a
   *                        {@link ResourceDefinition} can be generated. Must not be {@code null}.
   * @return {@code true} if this generator can generate a {@link ResourceDefinition} for the given
   * {@link TransferProcess} and {@link Policy}, {@code false} otherwise.
   * @throws NullPointerException if {@code transferProcess} is {@code null} or if {@code policy} is
   *                              {@code null}.
   */
  @Override
  public boolean canGenerate(TransferProcess transferProcess, Policy policy)
      throws NullPointerException {
    Objects.requireNonNull(transferProcess, "transferProcess must always be provided");
    Objects.requireNonNull(policy, "policy must always be provided");

    return OVHCloudBucketSchema.TYPE.equals(transferProcess.getDestinationType());
  }
}
