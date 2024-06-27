package com.ovhcloud.edc.provision.s3;

import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.BUCKET_NAME;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.OBJECT_NAME;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.PATH;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.REGION;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.TYPE;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedDataDestinationResource;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a provisioned resource for an OVHcloud S3 bucket. It contains the region and the
 * bucket name.
 */
@JsonDeserialize(builder = OVHCloudS3BucketProvisionedResource.Builder.class)
@JsonTypeName("dataspaceconnector:ovhclouds3bucketprovisionedresource")
public class OVHCloudS3BucketProvisionedResource extends ProvisionedDataDestinationResource {

  /**
   * Returns current region of the bucket.
   *
   * @return the region of the bucket.
   */
  public String getRegion() {
    return getDataAddress().getStringProperty(REGION);
  }

  /**
   * Returns the name of the bucket.
   *
   * @return the name of the bucket.
   */
  public String getBucketName() {
    return getDataAddress().getStringProperty(BUCKET_NAME);
  }

  /**
   * Returns the name of the resource.
   *
   * @return the name of the resource.
   */
  @Override
  public String getResourceName() {
    return getBucketName();
  }

  private OVHCloudS3BucketProvisionedResource() {
  }

  /**
   * Builder for {@link OVHCloudS3BucketProvisionedResource}.
   */
  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder extends
      ProvisionedDataDestinationResource.Builder<OVHCloudS3BucketProvisionedResource, Builder> {

    private Builder() {
      super(new OVHCloudS3BucketProvisionedResource());
      dataAddressBuilder.type(TYPE);
    }


    /**
     * Creates a new instance of the builder.
     * @return a new instance of the builder.
     */
    @Contract(" -> new")
    @JsonCreator
    public static @NotNull Builder newInstance() {
      return new Builder();
    }

    /**
     * Sets the region of the bucket.
     * @param region the region of the bucket.
     * @return the builder.
     */
    public Builder region(String region) {
      dataAddressBuilder.property(REGION, region);
      return this;
    }

    /**
     * Sets the name of the bucket.
     * @param bucketName the name of the bucket.
     * @return the builder.
     */
    public Builder bucketName(String bucketName) {
      dataAddressBuilder.property(BUCKET_NAME, bucketName);
      return this;
    }

    public Builder objectName(String objectName) {
      dataAddressBuilder.property(OBJECT_NAME, objectName);
      return this;
    }

    public Builder path(String path) {
      dataAddressBuilder.property(PATH, path);
      return this;
    }

  }
}
