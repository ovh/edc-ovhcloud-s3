package com.ovhcloud.edc.extension.s3.settings;

import org.eclipse.edc.runtime.metamodel.annotation.Setting;

/**
 * The schema for the S3 settings.
 */
public final class S3SettingsSchema {

  /**
   * The key of the secret where the S3 Access Key Id is stored.
   */
  @Setting(value = "The key of the secret where the S3 Access Key Id is stored")
  public static final String EDC_OVHCLOUD_S3_ACCESS_KEY = "edc.ovhcloud.s3.access.key";

  /**
   * The key of the secret where the S3 Secret Key Id is stored.
   */
  @Setting(value = "The key of the secret where the S3 Secret Key Id is stored")
  public static final String EDC_OVHCLOUD_S3_SECRET_KEY = "edc.ovhcloud.s3.secret.key";

  /**
   * The key of the secret where the S3 Endpoint is stored.
   */
  @Setting(value = "The key of the secret where the S3 Endpoint is stored")
  public static final String EDC_OVHCLOUD_S3_ENDPOINT = "edc.ovhcloud.s3.endpoint";

  private S3SettingsSchema() {
  }
}
