package com.ovhcloud.edc.provision;

import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import com.ovhcloud.edc.provision.s3.OVHCloudS3BucketProvisionedResource;
import com.ovhcloud.edc.provision.s3.OVHCloudS3ConsumerResourceDefinitionGenerator;
import com.ovhcloud.edc.provision.s3.OVHCloudS3Provisioner;
import com.ovhcloud.edc.provision.s3.OVHCloudS3ResourceDefinition;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import java.util.Optional;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;

/**
 * Provides data transfer {@link Provisioner}s backed by OVHcloud services.
 */
@Extension(value = OVHCloudProvisionExtension.NAME)
public class OVHCloudProvisionExtension implements ServiceExtension {

  /**
   * The name of this service extension.
   */
  public static final String NAME = "OVHcloud Provision";

  /**
   * The {@link Vault} service.
   */
  @Inject
  private Vault vault;

  /**
   * The {@link Monitor} service.
   */
  @Inject
  private Monitor monitor;

  /**
   * The {@link TypeManager} service.
   */
  @Inject
  private TypeManager typeManager;

  /**
   * The {@link S3ConnectorAPI} service.
   */
  @Inject
  private S3ConnectorAPI client;

  /**
   * The name of the {@link Provisioner} service.
   *
   * @return the name of this service extension.
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * Initializes this service extension.
   *
   * @param context the service extension context.
   */
  @Override
  public void initialize(ServiceExtensionContext context) {
    monitor = context.getMonitor();

    Optional.ofNullable(monitor)
        .ifPresent(
            m -> m.debug("Initializing OVHcloud Provision extension"));

    var provisionManager = context.getService(ProvisionManager.class);
    var retryPolicy = context.getService(RetryPolicy.class);
    var bucketProvisioner = new OVHCloudS3Provisioner(retryPolicy, monitor, client);

    Optional.ofNullable(monitor)
        .ifPresent(
            m -> {
              m.debug("Registering OVHcloud S3 Provisioner with ProvisionManager");
              m.debug("Registering OVHcloud S3 Provisioner with RetryPolicy");
              m.debug("Registering OVHcloud S3 Provisioner with OVHCloudS3Provisioner");
            });
    provisionManager.register(bucketProvisioner);

    var manifestGenerator = context.getService(ResourceManifestGenerator.class);
    manifestGenerator.registerGenerator(new OVHCloudS3ConsumerResourceDefinitionGenerator());

    typeManager.registerTypes(OVHCloudS3BucketProvisionedResource.class,
        OVHCloudS3ResourceDefinition.class);
  }

}
