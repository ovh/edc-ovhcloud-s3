package com.ovhcloud.edc.provision.s3;

import static dev.failsafe.Failsafe.with;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.jetbrains.annotations.NotNull;

import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;

import dev.failsafe.RetryPolicy;

/**
 * A {@link Provisioner} implementation for OVHCloud S3 buckets.
 */
public class OVHCloudS3Provisioner implements
    Provisioner<OVHCloudS3ResourceDefinition, OVHCloudS3BucketProvisionedResource> {

  private final RetryPolicy<Object> retryPolicy;
  private final Monitor monitor;
  private final S3ConnectorAPI ovhCloudS3Client;

  /**
   * Creates a new {@link OVHCloudS3Provisioner}.
   * 
   * @param retryPolicy      the {@link RetryPolicy} to use.
   * @param monitor          the {@link Monitor} to use.
   * @param ovhCloudS3Client the {@link S3ConnectorAPI} to use.
   */
  public OVHCloudS3Provisioner(RetryPolicy<Object> retryPolicy, Monitor monitor,
      S3ConnectorAPI ovhCloudS3Client) {
    this.retryPolicy = retryPolicy;
    this.monitor = monitor;
    this.ovhCloudS3Client = ovhCloudS3Client;
  }

  /**
   * Determines whether this provisioner can provision a
   * {@link ResourceDefinition}.
   * 
   * @param resourceDefinition the {@link ResourceDefinition} to check.
   * @return {@code true} if this provisioner can provision the
   *         {@link ResourceDefinition}.
   */
  @Override
  public boolean canProvision(ResourceDefinition resourceDefinition) {
    return resourceDefinition instanceof OVHCloudS3ResourceDefinition;
  }

  /**
   * Determines whether this provisioner can deprovision a
   * {@link ProvisionedResource}.
   * 
   * @param provisionedResource the {@link ProvisionedResource} to check.
   * @return {@code true} if this provisioner can deprovision the
   *         {@link ProvisionedResource}.
   */
  @Override
  public boolean canDeprovision(ProvisionedResource provisionedResource) {
    return provisionedResource instanceof OVHCloudS3BucketProvisionedResource;
  }

  /**
   * Provisions a new OVHCloud S3 bucket. If the bucket already exists, it will be
   * reused.
   * 
   * @param ovhCloudS3ResourceDefinition the {@link OVHCloudS3ResourceDefinition}
   *                                     to provision.
   * @param policy                       the {@link Policy} to use.
   * @return a {@link CompletableFuture} that will complete with the
   *         {@link ProvisionResponse}.
   */
  @Override
  public CompletableFuture<StatusResult<ProvisionResponse>> provision(
      OVHCloudS3ResourceDefinition ovhCloudS3ResourceDefinition, Policy policy) {

    var bucketName = ovhCloudS3ResourceDefinition.getBucketName();

    Optional.ofNullable(monitor)
        .ifPresent(m -> m.info("Provisioning request submitted for bucket: " + bucketName));

    return with(retryPolicy)
        .getAsync(() -> ovhCloudS3Client.bucketExists(bucketName))
        .thenCompose(exists -> {
          if (Boolean.TRUE.equals(exists)) {
            return reuseExistingBucket(bucketName);
          } else {
            return createBucket(bucketName);
          }
        })
        .thenApply(empty -> {
          var resourceName = ovhCloudS3ResourceDefinition.getId() + "-" + OffsetDateTime.now();

          Optional.ofNullable(monitor)
              .ifPresent(m -> {
                m.debug("OVHCloudS3Provisioner: provisioned bucket " + bucketName);
                m.debug("OVHCloudS3Provisioner: with resource name" + resourceName);
              });

          var resourceBuilder = OVHCloudS3BucketProvisionedResource.Builder.newInstance()
              .id(ovhCloudS3ResourceDefinition.getId())
              .bucketName(bucketName)
              .region(ovhCloudS3ResourceDefinition.getRegion())
              .resourceDefinitionId(ovhCloudS3ResourceDefinition.getId())
              .transferProcessId(ovhCloudS3ResourceDefinition.getTransferProcessId())
              .resourceName(resourceName);

          if (ovhCloudS3ResourceDefinition.getPath().isPresent()) {
            resourceBuilder.path(ovhCloudS3ResourceDefinition.getPath().get());
          }

          if (ovhCloudS3ResourceDefinition.getObjectName().isPresent()) {
            resourceBuilder.objectName(ovhCloudS3ResourceDefinition.getObjectName().get());
          }

          var response = ProvisionResponse.Builder.newInstance().resource(resourceBuilder.build()).build();
          return StatusResult.success(response);
        });
  }

  /**
   * Deprovisions an OVHCloud S3 bucket. The bucket will be deleted.
   * 
   * @param ovhCloudS3BucketProvisionedResource the
   *                                            {@link OVHCloudS3BucketProvisionedResource}
   *                                            to deprovision.
   * @param policy                              the {@link Policy} to use.
   * @return a {@link CompletableFuture} that will complete with the
   *         {@link DeprovisionedResource}.
   */
  @Override
  public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(
      OVHCloudS3BucketProvisionedResource ovhCloudS3BucketProvisionedResource, Policy policy) {

    var bucketName = ovhCloudS3BucketProvisionedResource.getBucketName();
    Optional.ofNullable(monitor)
        .ifPresent(m -> m.info("Deprovisioning request submitted for bucket: " + bucketName));

    return with(retryPolicy)
        .getAsync(() -> ovhCloudS3Client.listObjects(bucketName, ""))
        .thenCompose((listObjectsResponse) -> deleteObjects(bucketName, listObjectsResponse))
        .thenApply(empty -> {
          ovhCloudS3Client.deleteBucket(bucketName);
          return StatusResult.success(DeprovisionedResource.Builder.newInstance()
              .provisionedResourceId(ovhCloudS3BucketProvisionedResource.getId())
              .build());
        });
  }

  @NotNull
  private CompletableFuture<Void> reuseExistingBucket(String bucketName) {
    Optional.ofNullable(monitor).ifPresent(m -> m.info("Reusing existing bucket " + bucketName));
    return CompletableFuture.completedFuture(null);
  }

  @NotNull
  private CompletableFuture<Void> createBucket(String bucketName) {
    return with(retryPolicy)
        .runAsync(() -> {
          ovhCloudS3Client.createBucket(bucketName);
          Optional.ofNullable(monitor).ifPresent(
              m -> m.debug("OVHCloudS3Provisioner: created a new container " + bucketName));
        });
  }

  @NotNull
  private CompletableFuture<List<String>> listObjects(String bucketName, String prefix) {
    return with(retryPolicy).getAsync(() -> ovhCloudS3Client.listObjects(bucketName, prefix));
  }

  @NotNull
  private CompletableFuture<Void> deleteObjects(String bucketName, List<String> keys) {
    return with(retryPolicy).runAsync(() -> ovhCloudS3Client.deleteObjects(bucketName, keys));
  }

  @NotNull
  private CompletableFuture<Void> deleteBucket(String bucketName) {
    return with(retryPolicy).runAsync(() -> ovhCloudS3Client.deleteBucket(bucketName));
  }
}
