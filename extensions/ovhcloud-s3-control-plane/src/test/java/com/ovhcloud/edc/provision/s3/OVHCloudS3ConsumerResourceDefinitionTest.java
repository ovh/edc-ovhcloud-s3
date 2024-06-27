package com.ovhcloud.edc.provision.s3;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class OVHCloudS3ConsumerResourceDefinitionTest {

  @Test
  void builderShouldThrowExceptionWhenRegionIsNull() {
      assertThatThrownBy(() -> OVHCloudS3ResourceDefinition.Builder.newInstance()
              .id("id")
              .transferProcessId("tp-id")
              .bucketName("bucket")
              .build())
              .isInstanceOf(NullPointerException.class)
              .hasMessage("region is required");
  }

  @Test
  void builderShouldThrowExceptionWhenBucketNameIsNull() {
      assertThatThrownBy(() -> OVHCloudS3ResourceDefinition.Builder.newInstance()
              .id("id")
              .transferProcessId("tp-id")
              .region("region")
              .build())
              .isInstanceOf(NullPointerException.class)
              .hasMessage("bucketName is required");
  }

  @Test
  void toBuilderShouldReturnsTheSameObject() {
      var definition = OVHCloudS3ResourceDefinition.Builder.newInstance()
              .id("id")
              .transferProcessId("tp-id")
              .region("region")
              .bucketName("bucket")
              .build();

      var builder = definition.toBuilder();
      var rebuiltDefinition = builder.build();

      assertThat(rebuiltDefinition).usingRecursiveComparison().isEqualTo(definition);
  }
}
