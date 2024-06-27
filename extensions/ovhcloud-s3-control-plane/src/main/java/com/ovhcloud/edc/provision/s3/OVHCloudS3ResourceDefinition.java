package com.ovhcloud.edc.provision.s3;

import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import java.util.Objects;
import java.util.Optional;

/**
 * Resource definition for OVHCloud S3 bucket consumer.
 */
public class OVHCloudS3ResourceDefinition extends ResourceDefinition {

  private String region;
  private String bucketName;
  private Optional<String> objectName;
  private Optional<String> path;

  /**
   * Return a new Builder instance to build a OVHCloudS3ConsumerResourceDefinition.
   *
   * @return a new Builder instance
   */
  @Override
  public Builder toBuilder() {
    return initializeBuilder(new Builder())
        .bucketName(bucketName)
        .region(region)
        .objectName(objectName)
        .path(path);
}

  /**
   * Get the region of the S3 bucket.
   *
   * @return the region of the S3 bucket.
   */
  public String getRegion() {
    return region;
  }

  /**
   * Get the name of the S3 bucket.
   *
   * @return the name of the S3 bucket.
   */
  public String getBucketName() {
    return bucketName;
  }


  public Optional<String> getObjectName() {
    return objectName;
  }

  public Optional<String> getPath() {
    return path;
  }


  /**
   * Builder for OVHCloudS3ConsumerResourceDefinition.
   */
  public static class Builder extends
      ResourceDefinition.Builder<OVHCloudS3ResourceDefinition, Builder> {

    private Builder() {
      super(new OVHCloudS3ResourceDefinition());
    }

    /**
     * Create a new instance of the builder.
     *
     * @return a new instance of the builder
     */
    public static Builder newInstance() {
      return new Builder();
    }

    /**
     * Sets the region of the S3 bucket in the builder instance.
     *
     * @param region the region of the S3 bucket
     * @return the builder instance
     */
    public Builder region(String region) {
      resourceDefinition.region = region;
      return this;
    }

    /**
     * Sets the name of the S3 bucket in the builder instance.
     *
     * @param bucketName the name of the S3 bucket
     * @return the builder instance
     */
    public Builder bucketName(String bucketName) {
      resourceDefinition.bucketName = bucketName;
      return this;
    }

    public Builder objectName(Optional<String> objectName) {
      resourceDefinition.objectName = objectName;

      return this;
    }

    public Builder path(Optional<String> path) {
      resourceDefinition.path = path;

      return this;
    }

    /**
     * Checks if the required fields are set in the builder instance. If not, throws a
     * NullPointerException.
     *
     * @throws NullPointerException if one of the required fields are not set.
     */
    @Override
    protected void verify() {
      super.verify();
      Objects.requireNonNull(resourceDefinition.region, "region is required");
      Objects.requireNonNull(resourceDefinition.bucketName, "bucketName is required");
    }
  }
}
