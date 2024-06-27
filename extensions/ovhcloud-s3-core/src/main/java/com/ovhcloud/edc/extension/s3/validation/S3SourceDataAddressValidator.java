package com.ovhcloud.edc.extension.s3.validation;

import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.BUCKET_NAME;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.OBJECT_NAME;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.OBJECT_PREFIX;
import static com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema.REGION;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;

/**
 * Validator for S3 data addresses containing OVHcloud S3 source data. This validator checks that the
 * required fields (bucket_name, region, object_name and object_prefix) are not null or empty.
 */
public class S3SourceDataAddressValidator extends S3DataAddressValidator implements
        Validator<DataAddress> {

    /**
     * Validate the given data address. The fields to validate are the bucket_name, region, object_name and object_prefix. If one
     * of these fields is invalid, a violation is returned and the validation fails.
     *
     * @param dataAddress the data address to validate
     * @return the validation result. If the validation is successful, the result is a success. If the validation fails,
     * the result is a failure with the list of violations.
     */
    @Override
    public ValidationResult validate(DataAddress dataAddress) {
        return validate(dataAddress, BUCKET_NAME, OBJECT_NAME);

    }
}