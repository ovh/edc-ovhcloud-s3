package com.ovhcloud.edc.provision.s3;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudRegions;
import java.util.UUID;
import java.util.stream.Stream;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.aggregator.ArgumentsAggregationException;
import org.junit.jupiter.params.aggregator.ArgumentsAggregator;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

public class OVHCloudS3ConsumerResourceDefinitionGeneratorTest {
  private OVHCloudS3ConsumerResourceDefinitionGenerator generator;

  @BeforeEach
  void setUp() {
      generator = new OVHCloudS3ConsumerResourceDefinitionGenerator();
  }

  @ParameterizedTest
  @MethodSource("provideValidBucketNamesAndRegions")
  public void canGenerateShouldReturnValidResourceDefinition(@AggregateWith(TransferProcessAggregator.class) TransferProcess transferProcess) {
        var policy = Policy.Builder.newInstance().build();

        assertTrue(generator.canGenerate(transferProcess, policy));
  }

  @ParameterizedTest
  @NullSource
  public void canGenerateShouldThrowNullPointerExceptionIfTransferProcessIsNull(TransferProcess transferProcess) {
    var policy = Policy.Builder.newInstance().build();
    assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.canGenerate(transferProcess, policy));
  }

  @ParameterizedTest
  @NullSource
  public void canGenerateShouldThrowNullPointerExceptionIfPolicyIsNull(Policy policy) {
    var destination = DataAddress.Builder.newInstance().type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "bucket")
        .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
        .build();

    var asset = Asset.Builder.newInstance().build();
    var transferProcess = TransferProcess.Builder.newInstance()
            .dataDestination(destination)
            .assetId(asset.getId())
            .correlationId("process-id")
            .build();
    assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.canGenerate(transferProcess, policy));
  }

  @ParameterizedTest
  @MethodSource("provideValidBucketNamesAndRegions")
  public void generateShouldReturnValidResourceDefinition(@AggregateWith(TransferProcessAggregator.class) TransferProcess transferProcess) {
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);
        assertThat(definition).isInstanceOf(OVHCloudS3ResourceDefinition.class);

        var objectDef = (OVHCloudS3ResourceDefinition) definition;
        assertThat(objectDef.getBucketName()).isEqualTo(transferProcess.getDataDestination().getStringProperty(OVHCloudBucketSchema.BUCKET_NAME));
        assertThat(objectDef.getRegion()).isEqualTo(transferProcess.getDataDestination().getStringProperty(OVHCloudBucketSchema.REGION));
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
  }

  @ParameterizedTest
  @NullSource
  public void generateShouldThrowNullPointerExceptionIfTransferProcessIsNull(TransferProcess transferProcess) {
    var policy = Policy.Builder.newInstance().build();
    assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(transferProcess, policy));
  }

  @ParameterizedTest
  @NullSource
  public void generateShouldThrowNullPointerExceptionIfPolicyIsNull(Policy policy) {
    var destination = DataAddress.Builder.newInstance().type(OVHCloudBucketSchema.TYPE)
        .property(OVHCloudBucketSchema.BUCKET_NAME, "bucket")
        .property(OVHCloudBucketSchema.REGION, OVHCloudRegions.DEFAULT_REGION)
        .build();

    var asset = Asset.Builder.newInstance().build();
    var transferProcess = TransferProcess.Builder.newInstance()
            .dataDestination(destination)
            .assetId(asset.getId())
            .correlationId("process-id")
            .build();
    assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(transferProcess, policy));
  }

  private static @NotNull Stream<Arguments> provideValidBucketNamesAndRegions() {
    return Stream.of(
        Arguments.of("default-bucket", OVHCloudRegions.DEFAULT_REGION),
        Arguments.of("default-bucket", OVHCloudRegions.GRAVELINES),
        Arguments.of("bucket", OVHCloudRegions.STRASBOURG),
        Arguments.of("default-bucket", OVHCloudRegions.LONDON)
    );
  }
}

class TransferProcessAggregator implements ArgumentsAggregator {
    @Override
    public Object aggregateArguments(@NotNull ArgumentsAccessor accessor, ParameterContext context)
      throws ArgumentsAggregationException {
        var destination = DataAddress.Builder.newInstance().type(OVHCloudBucketSchema.TYPE)
                .property(OVHCloudBucketSchema.BUCKET_NAME, accessor.getString(0))
                .property(OVHCloudBucketSchema.REGION, accessor.getString(1))
                .build();

        var asset = Asset.Builder.newInstance().build();
        return TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
    }
}