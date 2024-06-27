package com.ovhcloud.edc.extension.s3.validator;

import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.validation.S3DestinationDataAddressValidator;
import com.ovhcloud.edc.extension.s3.validation.S3SourceDataAddressValidator;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3DestinationDataAddressValidatorTest {

  @Test
  public void destinationDataAddressValidatorShouldSucceededWhenPropertiesAreSet() {
    S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "testBucket")
        .property(OVHCloudBucketSchema.REGION, "testRegion")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.succeeded());
    assertTrue(result.getFailureMessages().isEmpty());
  }

  @Test
  public void destinationDataAddressValidatorShouldFailsWhenBucketNamePropertyNotSet() {
    S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.REGION, "testRegion")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.BUCKET_NAME + " is required"));
  }

  @Test
  public void destinationDataAddressValidatorShouldFailsWhenRegionPropertyNotSet() {
    S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "testBucket")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.REGION + " is required"));
  }

  @Test
  public void destinationDataAddressValidatorShouldFailsWhenMandatoryPropertiesNotSet() {
    S3DestinationDataAddressValidator validator = new S3DestinationDataAddressValidator();
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
            "The field " + OVHCloudBucketSchema.REGION + " is required"
        )
    );
  }
}
