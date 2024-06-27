package com.ovhcloud.edc.extension.s3.utils;

import io.minio.MinioClient;

/**
 * This interface defines methods to be able to build a MinioClient.
 */
public interface MinioClientBuilder {

  /**
   * Build and return a MinioClient.
   *
   * @return a MinioClient
   */
  MinioClient build();

  /**
   * Set the endpoint of the OVHCloud S3 bucket.
   *
   * @param endpoint the endpoint
   * @return the MinioClientBuilder
   */
  MinioClientBuilder endpoint(String endpoint);

  /**
   * Set the region of the OVHCloud S3 bucket.
   *
   * @param region the region
   * @return the MinioClientBuilder
   */
  MinioClientBuilder region(String region);

  /**
   * Set the credentials to be able to connect to the OVHCloud S3 bucket.
   *
   * @param accessKey the access key
   * @param secretKey the secret key
   * @return the MinioClientBuilder
   */
  MinioClientBuilder credentials(String accessKey, String secretKey);
}
