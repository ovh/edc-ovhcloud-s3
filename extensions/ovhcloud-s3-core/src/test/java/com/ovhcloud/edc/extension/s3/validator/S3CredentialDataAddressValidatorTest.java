package com.ovhcloud.edc.extension.s3.validator;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.validation.S3CredentialDataAddressValidator;
import java.util.List;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.Test;

public class S3CredentialDataAddressValidatorTest {

  @Test
  public void credentialDataAddressValidatorShouldSucceededWhenPropertiesAreSet() {
    S3CredentialDataAddressValidator validator = new S3CredentialDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.ACCESS_KEY_ID, "testAccessKey")
        .property(OVHCloudBucketSchema.SECRET_ACCESS_KEY, "testSecretKey")
        .property(OVHCloudBucketSchema.ENDPOINT, "testEndpoint")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.succeeded());
    assertTrue(result.getFailureMessages().isEmpty());
  }

  @Test
  public void credentialDataAddressValidatorShouldFailsWhenAccessKeyPropertyNotSet() {
    S3CredentialDataAddressValidator validator = new S3CredentialDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.SECRET_ACCESS_KEY, "testSecretKey")
        .property(OVHCloudBucketSchema.ENDPOINT, "testEndpoint")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.ACCESS_KEY_ID + " is required"));
  }

  @Test
  public void credentialDataAddressValidatorShouldFailsWhenSecretKeyPropertyNotSet() {
    S3CredentialDataAddressValidator validator = new S3CredentialDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.ACCESS_KEY_ID, "testAccessKey")
        .property(OVHCloudBucketSchema.ENDPOINT, "testEndpoint")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.SECRET_ACCESS_KEY + " is required"));
  }

  @Test
  public void credentialDataAddressValidatorShouldFailsWhenEndpointPropertyNotSet() {
    S3CredentialDataAddressValidator validator = new S3CredentialDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.ACCESS_KEY_ID, "testAccessKey")
        .property(OVHCloudBucketSchema.SECRET_ACCESS_KEY, "testEndpoint")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(result.getFailureMessages(),
        List.of("The field " + OVHCloudBucketSchema.ENDPOINT + " is required"));
  }

  @Test
  public void credentialDataAddressValidatorShouldFailsWhenMandatoryPropertiesNotSet() {
    S3CredentialDataAddressValidator validator = new S3CredentialDataAddressValidator();
    DataAddress dataAddress = DataAddress.Builder.newInstance()
        .type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.REGION, "testAccessKey")
        .build();

    ValidationResult result = validator.validate(dataAddress);

    assertTrue(result.failed());
    assertIterableEquals(
        result.getFailureMessages(),
        List.of(
            "The field " + OVHCloudBucketSchema.ACCESS_KEY_ID + " is required",
            "The field " + OVHCloudBucketSchema.SECRET_ACCESS_KEY + " is required",
            "The field " + OVHCloudBucketSchema.ENDPOINT + " is required"
        )
    );
  }
}
