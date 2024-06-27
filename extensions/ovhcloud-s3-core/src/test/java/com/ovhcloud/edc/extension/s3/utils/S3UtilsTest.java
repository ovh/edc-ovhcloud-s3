package com.ovhcloud.edc.extension.s3.utils;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.internal.matchers.Null;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3UtilsTest {

  @ParameterizedTest
  @ValueSource(strings = {"endpoint", " ", "endpoint/", "/endpoint", "/endpoint/", "endpoint/ "})
  public void validateEndpointNotNullShouldNotThrowExceptionIfValidStringSubmitted(
      String endpoint) {
    assertDoesNotThrow(() -> S3Utils.validateEndpointNotNull(endpoint));
  }

  @ParameterizedTest
  @NullSource
  public void validateEndpointNotNullShouldThrowExceptionIfNullStringSubmitted(String endpoint) {
    assertThrows(NullPointerException.class, () -> S3Utils.validateEndpointNotNull(endpoint));
  }

  @ParameterizedTest
  @EmptySource
  public void validateEndpointNotEmptyShouldNotThrowExceptionIfNonEmptyStringSubmitted(
      String endpoint) {
    assertDoesNotThrow(() -> S3Utils.validateEndpointNotEmpty("endpoint"));
    assertDoesNotThrow(() -> S3Utils.validateEndpointNotEmpty(" "));
  }

  @ParameterizedTest
  @NullSource
  public void validateEndpointNotEmptyShouldThrowExceptionIfNullStringSubmitted(String endpoint) {
    assertThrows(NullPointerException.class,
        () -> S3Utils.validateEndpointNotEmpty(endpoint));
  }

  @ParameterizedTest
  @EmptySource
  public void validateEndpointNotEmptyShouldThrowExceptionIfEmptyStringSubmitted(String endpoint) {
    assertThrows(IllegalArgumentException.class,
        () -> S3Utils.validateEndpointNotEmpty(endpoint));
  }

  @ParameterizedTest
  @NullSource
  public void validateOVHEndpointShouldThrowExceptionIfNullStringSubmitted(String endpoint) {
    assertThrows(NullPointerException.class, () -> S3Utils.validateOVHEndpoint(endpoint));
  }

  @ParameterizedTest
  @EmptySource
  public void validateOVHEndpointShouldThrowExceptionIfEmptyStringSubmitted(String endpoint) {
    assertThrows(IllegalArgumentException.class, () -> S3Utils.validateOVHEndpoint(""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3.region.cloud.ovh.net", "s3.region.io.cloud.ovh.net/",
      "s3.region.io.cloud.ovh.net ", "s3.region.io.cloud.ovh.net/endpoint",
      "s3.region.io.cloud.ovh.net/endpoint/", "s3.region.io.cloud.ovh.net/endpoint/ "})
  public void validateOVHEndpointShouldThrowExceptionIfNonValidURLSubmitted(String endpoint) {
    assertThrows(IllegalArgumentException.class, () -> S3Utils.validateOVHEndpoint(endpoint));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidEndpoints")
  public void validateOVHEndpointShouldThrowExceptionIfNonWellFormattedURLSubmitted(
      String endpoint) {
    assertThrows(IllegalArgumentException.class, () -> S3Utils.validateOVHEndpoint(endpoint));
  }

  @ParameterizedTest
  @MethodSource("provideValidEndpoints")
  public void validateOVHEndpointShouldNotThrowExceptionIfWellFormattedURLSubmitted(
      String endpoint) {
    assertDoesNotThrow(() -> S3Utils.validateOVHEndpoint(endpoint));
  }

  @ParameterizedTest
  @MethodSource("provideValidEndpoints")
  public void extractRegionFromEndpointShouldGiveCorrectRegionIfWellFormattedURLSubmitted(
      String endpoint, String expected) {

    assertDoesNotThrow(() -> S3Utils.extractRegionFromEndpoint(endpoint));
    assertEquals(expected, S3Utils.extractRegionFromEndpoint(endpoint));

  }

  private static @NotNull Stream<Arguments> provideValidEndpoints() {
    return Stream.of(
        Arguments.of("https://s3.region.io.cloud.ovh.net", "region"),
        Arguments.of("https://s3.a.io.cloud.ovh.net", "a"),
        Arguments.of("https://s3.ab.io.cloud.ovh.net", "ab"),
        Arguments.of("https://s3.gra.io.cloud.ovh.net", "gra")
    );
  }
  private static @NotNull Stream<String> provideInvalidEndpoints() {
    return Stream.of(
        "s3.io.cloud.ovh.net",
        "https://s3.123.cloud.ovh.net",
        "https://s3.abc1.cloud.ovh.net",
        "https://s3.abcé.cloud.ovh.net",
        "https://s3.region.cloud.ovh.net/",
        "https://s3.region.io.cloud.ovh.net ",
        "https://s3.region.io.cloud.ovh.net/endpoint",
        "https://s3.region.io.cloud.ovh.net/endpoint/",
        "https://s3.region.io.cloud.ovh.net/endpoint/ ",
        "https://s3.io.cloud.ovh.net ",
        "http://s3.region.io.cloud.ovh.net",
        "https://s3.us-east-2.amazonaws.com"
    );
  }
}
