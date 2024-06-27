package com.ovhcloud.edc.extension.s3.validator;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.validation.S3SourceDataAddressValidator;
import java.util.List;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.Test;

public class S3SourceDataAddressValidatorTest {

  @Test
  public void sourceDataAddressValidatorShouldSucceededWhenPropertiesAreSet() {
    S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "testBucket")
        .property(OVHCloudBucketSchema.REGION, "testRegion")
        .property(OVHCloudBucketSchema.OBJECT_NAME, "testObject")
        .property(OVHCloudBucketSchema.OBJECT_PREFIX, "testPrefix")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.succeeded());
    assertTrue(result.getFailureMessages().isEmpty());
  }

  @Test
  public void sourceDataAddressValidatorShouldFailsWhenBucketNamePropertyNotSet() {
    S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.REGION, "testRegion")
        .property(OVHCloudBucketSchema.OBJECT_NAME, "testObject")
        .property(OVHCloudBucketSchema.OBJECT_PREFIX, "testPrefix")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.BUCKET_NAME + " is required"));
  }

  @Test
  public void sourceDataAddressValidatorShouldFailsWhenObjectNamePropertyNotSet() {
    S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "testBucket")
        .property(OVHCloudBucketSchema.REGION, "testRegion")
        .property(OVHCloudBucketSchema.OBJECT_PREFIX, "testPrefix")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.OBJECT_NAME + " is required"));
  }

  @Test
  public void sourceDataAddressValidatorShouldFailsWhenMandatoryPropertiesNotSet() {
    S3SourceDataAddressValidator validator = new S3SourceDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.ACCESS_KEY_ID, "testAccessKey")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(
        result.getFailureMessages(),
        List.of(
            "The field " + OVHCloudBucketSchema.BUCKET_NAME + " is required",
            "The field " + OVHCloudBucketSchema.OBJECT_NAME + " is required"
        )
    );
  }
}
