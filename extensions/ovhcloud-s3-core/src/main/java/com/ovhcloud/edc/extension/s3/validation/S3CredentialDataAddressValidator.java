package com.ovhcloud.edc.extension.s3.validation;

import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.ACCESS_KEY_ID;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.ENDPOINT;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.SECRET_ACCESS_KEY;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;

/**
 * Validator for S3 data addresses containing OVHcloud S3 credentials. This validator checks that the
 * required fields (access_key_id, secret_access_key and endpoint) are not null or empty.
 */
public class S3CredentialDataAddressValidator extends S3DataAddressValidator implements
    Validator<DataAddress> {

  /**
   * Validate the given data address. The fields to validate are the access_key_id, secret_access_key
   * and endpoint.
   *
   * @param dataAddress the data address to validate
   * @return the validation result. If the validation is successful, the result is a success. If the
   * validation fails, the result is a failure with the list of violations.
   */
  @Override
  public ValidationResult validate(DataAddress dataAddress) {
    return validate(dataAddress, ACCESS_KEY_ID, SECRET_ACCESS_KEY, ENDPOINT);
  }
}