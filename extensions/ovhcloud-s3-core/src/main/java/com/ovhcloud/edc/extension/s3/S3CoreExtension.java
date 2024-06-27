package com.ovhcloud.edc.extension.s3;

import static com.ovhcloud.edc.extension.s3.settings.S3SettingsSchema.EDC_OVHCLOUD_S3_ACCESS_KEY;
import static com.ovhcloud.edc.extension.s3.settings.S3SettingsSchema.EDC_OVHCLOUD_S3_ENDPOINT;
import static com.ovhcloud.edc.extension.s3.settings.S3SettingsSchema.EDC_OVHCLOUD_S3_SECRET_KEY;

import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPIImpl;
import com.ovhcloud.edc.extension.s3.utils.MinioClientBuilderImpl;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * This class provides an implementation of the ServiceExtension interface to handle connection to
 * OVHCloud S3.
 */
@Provides(value = S3ConnectorAPI.class)
@Extension(value = S3CoreExtension.NAME)
public class S3CoreExtension implements ServiceExtension {

  /**
   * The name of this extension. It is used to identify the extension in the EDC system.
   */
  public static final String NAME = "OVHcloud_S3";

  /**
   * The monitor used for logging. It is automatically injected by the runtime.
   */
  @Inject
  private Monitor monitor;

  /**
   * Returns the name of this extension.
   *
   * @return the name of this extension
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * Initializes the extension. This method is responsible for retrieving the S3 credentials from
   * the configuration file, creating a MinioClient with these credentials, and registering the
   * S3ConnectorAPI service.
   *
   * @param context the ServiceExtensionContext
   */
  @Override
  public void initialize(ServiceExtensionContext context) {
    monitor.info("Initializing S3CoreExtension");
    monitor.debug("Getting S3 Credentials from configuration file");

    // Get S3 credentials from configuration file
    String accessKey = context.getSetting(EDC_OVHCLOUD_S3_ACCESS_KEY, "");
    String secretKey = context.getSetting(EDC_OVHCLOUD_S3_SECRET_KEY, "");
    String endpoint = context.getSetting(EDC_OVHCLOUD_S3_ENDPOINT, "");

    var minioClient = MinioClientBuilderImpl.builder()
        .credentials(accessKey, secretKey)
        .endpoint(endpoint)
        .build();

    context.registerService(S3ConnectorAPI.class, new S3ConnectorAPIImpl(minioClient, monitor));
  }
}