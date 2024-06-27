package com.ovhcloud.edc.extension.s3.schemas;

import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;

/**
 * OVHCloud S3 bucket schema. It contains the configuration keys for the OVHCloud S3 configuration
 * file.
 */
public interface OVHCloudBucketSchema {

  /**
   * The type of this extension schema.
   */
  String TYPE = "OVHCloudS3";

  /**
   * The name of the key that contains the bucket name.
   */
  String BUCKET_NAME = EDC_NAMESPACE + "bucketName";

  /**
   * The name of the key that contains the access key credential.
   */
  String ACCESS_KEY_ID = EDC_NAMESPACE + "accessKey";

  /**
   * The name of the key that contains the secret key credential.
   */
  String SECRET_ACCESS_KEY = EDC_NAMESPACE + "secretKey";

  /**
   * The name of the key that contains the endpoint.
   */
  String ENDPOINT = EDC_NAMESPACE + "endpoint";

  /**
   * The name of the key that contains the region.
   */
  String REGION = EDC_NAMESPACE + "region";

  /**
   * The name of the key that contains the object prefix.
   */
  String OBJECT_PREFIX = EDC_NAMESPACE + "objectPrefix";

  /**
   * The name of the key that contains the object name.
   */
  String OBJECT_NAME = EDC_NAMESPACE + "objectName";

  /**
   * The name of the key that contains the directory path.
   */
  String PATH = EDC_NAMESPACE + "path";

}
