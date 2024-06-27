package com.ovhcloud.edc.extension.s3.utils;

import org.jetbrains.annotations.NotNull;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;


/**
 * Utility class to validate and extract information from S3 endpoints.
 */
public final class S3Utils {

  /**
   * Regular expression to validate OVH Cloud S3 endpoints.
   * <p>
   * The endpoint must be in the form of https://s3.{region}.io.cloud.ovh.net where {region} is the
   * region of the S3 bucket. Example: https://s3.gra.io.cloud.ovh.net <br/> Note: This regular
   * expression does not validate the region part. It only checks if the endpoint is in the correct
   * format.
   * </p>
   */
  public static final String OVH_CLOUD_S3_ENDPOINT_REGEX = "^https://s3\\.([a-z]+)\\.io\\.cloud\\.ovh\\.net$";

  /**
   * Checks if the endpoint is not null. If it is null, a NullPointerException is thrown.
   *
   * @param endpoint the endpoint to validate
   * @throws NullPointerException if the endpoint is null
   */
  public static void validateEndpointNotNull(String endpoint) {
    Objects.requireNonNull(endpoint, "endpoint must not be null.");
  }

  /**
   * Checks if the endpoint is not empty. If it is empty, an IllegalArgumentException is thrown.
   *
   * @param endpoint the endpoint to validate
   * @throws IllegalArgumentException if the endpoint is empty
   */
  public static void validateEndpointNotEmpty(String endpoint) {
    validateEndpointNotNull(endpoint);
    if (endpoint.isEmpty()) {
      throw new IllegalArgumentException("endpoint must not be empty.");
    }
  }

  /**
   * Validates an OVH Cloud S3 endpoint. The endpoint must not be null, not empty, and match the
   * regular expression OVH_CLOUD_S3_ENDPOINT_REGEX.
   * <p>  The endpoint must be in the form of https://s3.{region}.io.cloud.ovh.net where {region}
   * is the region of the S3 bucket. Example: https://s3.gra.io.cloud.ovh.net Supported regions:
   * gra, sbg, bhs, waw, sgp, syd, nrt, ewr, iad, fra, lon, par
   * </p>
   *
   * @param endpoint the endpoint to validate
   * @throws NullPointerException     if the endpoint is null
   * @throws IllegalArgumentException if the endpoint is empty or not a valid OVH Cloud S3 endpoint
   */
  public static void validateOVHEndpoint(String endpoint) {
    validateEndpointNotEmpty(endpoint);
    if (!endpoint.matches(OVH_CLOUD_S3_ENDPOINT_REGEX)) {
      throw new IllegalArgumentException("endpoint must be a valid OVH Cloud S3 endpoint.");
    }
  }

  /**
   * Extracts the region from an OVH Cloud S3 endpoint.
   * <p>
   * The endpoint must be in the form of https://s3.{region}.io.cloud.ovh.net where {region} is the
   * region of the S3 bucket. Example: https://s3.gra.io.cloud.ovh.net
   * </p>
   *
   * @param endpoint the endpoint to extract the region from
   * @return the region extracted from the endpoint
   * @throws IllegalArgumentException if the endpoint is not a valid OVH Cloud S3 endpoint
   */
  public static String extractRegionFromEndpoint(@NotNull String endpoint) {
    validateOVHEndpoint(endpoint);
    try {
      var url = new URL(endpoint);
      String host = url.getHost();

      String[] parts = host.split("\\.");
      return parts[1];
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("endpoint must be a valid OVH Cloud S3 endpoint.");
    }
  }
}
