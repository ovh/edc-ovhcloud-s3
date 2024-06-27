package com.ovhcloud.edc.dataplane;

import com.ovhcloud.edc.dataplane.s3.OVHCloudS3DataSinkFactory;
import com.ovhcloud.edc.dataplane.s3.OVHCloudS3DataSourceFactory;
import com.ovhcloud.edc.extension.s3.api.S3ConnectorAPI;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import java.util.concurrent.Executors;

@Extension(value = OVHCloudDataPlaneExtension.NAME)
public class OVHCloudDataPlaneExtension implements ServiceExtension {

  public static final String NAME = "Data plane - OVHCloud S3";

  @Inject
  private PipelineService pipelineService;

  @Inject
  private S3ConnectorAPI s3Client;

  @Inject
  private DataTransferExecutorServiceContainer executorContainer;

  @Inject
  private Vault vault;

  @Inject
  private TypeManager typeManager;

  @Inject
  private Monitor monitor;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(ServiceExtensionContext context) {
    monitor.debug("Initializing OVHCloud Provision extension");
    var sourceFactory = new OVHCloudS3DataSourceFactory(s3Client, monitor);
    pipelineService.registerFactory(sourceFactory);

    var sinkFactory = new OVHCloudS3DataSinkFactory(s3Client, monitor, Executors.newFixedThreadPool(10));
    pipelineService.registerFactory(sinkFactory);
    monitor.debug("OVHCloud Provision extension initialized");
  }
}
