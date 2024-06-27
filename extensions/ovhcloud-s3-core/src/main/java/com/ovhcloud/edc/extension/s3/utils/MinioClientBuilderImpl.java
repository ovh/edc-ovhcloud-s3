package com.ovhcloud.edc.extension.s3.utils;

import io.minio.BucketExistsArgs.Builder;
import io.minio.MinioClient;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import java.util.Optional;

import static com.ovhcloud.edc.extension.s3.utils.S3Utils.extractRegionFromEndpoint;

/**
 * Default implementation of MinioClientBuilder interface. It is used to build MinioClient
 * instances.
 */
public class MinioClientBuilderImpl implements MinioClientBuilder {

  private String accessKey = null;
  private String secretKey = null;
  private String endpoint = null;
  private String region = null;

  /**
   * Creates a new instance of MinioClientBuilderImpl.
   */
  @Contract(value = " -> new", pure = true)
  public static @NotNull MinioClientBuilder builder() {
    return new MinioClientBuilderImpl();
  }

  /**
   * Set the endpoint of the OVHCloud S3 bucket.
   *
   * @param endpoint the endpoint
   * @return the MinioClientBuilder
   */
  @Override
  public MinioClientBuilder endpoint(String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  /**
   * Set the region of the OVHCloud S3 bucket.
   *
   * @param region the region
   * @return the MinioClientBuilder
   */
  @Override
  public MinioClientBuilder region(String region) {
    this.region = region;
    return this;
  }

  /**
   * Set the credentials to be able to connect to the OVHCloud S3 bucket.
   *
   * @param accessKey the access key
   * @param secretKey the secret key
   * @return the MinioClientBuilder
   */
  @Override
  public MinioClientBuilder credentials(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    return this;
  }

  /**
   * Build and return a MinioClient.
   *
   * @return a MinioClient
   */
  @Override
  public @NotNull MinioClient build() {
    return MinioClient
        .builder()
        .credentials(accessKey, secretKey)
        .endpoint(endpoint)
        .region(Optional.ofNullable(region).orElse(extractRegionFromEndpoint(endpoint)))
        .build();
  }
}