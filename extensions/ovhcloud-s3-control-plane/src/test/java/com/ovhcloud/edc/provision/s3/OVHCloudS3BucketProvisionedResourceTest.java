package com.ovhcloud.edc.provision.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudBucketSchema;
import com.ovhcloud.edc.extension.s3.schemas.OVHCloudRegions;
import java.io.File;
import java.io.IOException;
import org.eclipse.edc.json.JacksonTypeManager;
import org.junit.jupiter.api.Test;

public class OVHCloudS3BucketProvisionedResourceTest {

  private final ObjectMapper mapper = new JacksonTypeManager().getMapper();

  @Test
  void deserializeShouldGetValidObjectWhenJsonStringIsValid() throws IOException {
    String jsonString ="{\"edctype\":\"dataspaceconnector:ovhclouds3bucketprovisionedresource\",\"id\":\"37dee866-5001-4a5f-ad9e-bd0a54cc5a94\",\"transferProcessId\":\"123\",\"resourceDefinitionId\":\"4e421ee6-4df5-4616-b106-89a160918d2e\",\"resourceName\":\"bucket\",\"dataAddress\":{\"properties\":{\"bucketName\":\"bucket\",\"https://w3id.org/edc/v0.0.1/ns/type\":\"OVHCloudS3\",\"region\":\"gra\"}},\"region\":\"gra\",\"bucketName\":\"bucket\"}";

    OVHCloudS3BucketProvisionedResource provisionedResource = mapper.readValue(jsonString,
        OVHCloudS3BucketProvisionedResource.class);

    assertNotNull(provisionedResource);
    assertEquals(OVHCloudRegions.DEFAULT_REGION, provisionedResource.getRegion());
    assertEquals("bucket", provisionedResource.getBucketName());
    assertEquals("bucket", provisionedResource.getResourceName());
    assertEquals("37dee866-5001-4a5f-ad9e-bd0a54cc5a94", provisionedResource.getId());
    assertEquals("123", provisionedResource.getTransferProcessId());
    assertEquals("4e421ee6-4df5-4616-b106-89a160918d2e", provisionedResource.getResourceDefinitionId());

    var dataAddress = provisionedResource.getDataAddress();
    assertNotNull(dataAddress);
    assertEquals("OVHCloudS3", dataAddress.getType());
    assertEquals("bucket", dataAddress.getStringProperty(OVHCloudBucketSchema.BUCKET_NAME));
    assertEquals("gra", dataAddress.getStringProperty(OVHCloudBucketSchema.REGION));
  }

  @Test
  void deserializeShouldThrowErrorWhenJsonStringIsNull() {
    assertThrows(IllegalArgumentException.class, () -> {
      mapper.readValue((File) null, OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenJsonStringIsEmpty() {
    assertThrows(JsonMappingException.class, () -> {
      mapper.readValue("", OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenJsonStringIsInvalid() {
    assertThrows(JsonParseException.class, () -> {
      mapper.readValue("invalid json", OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenBucketNamePropertyIsMissing() {
    String jsonString = String.format("{\"%s\":\"%s\"}", OVHCloudBucketSchema.REGION,
        OVHCloudRegions.DEFAULT_REGION); // bucketName property is missing

    assertThrows(JsonMappingException.class, () -> {
      mapper.readValue(jsonString, OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenRegionPropertyIsMissing() {
    String jsonString = String.format("{\"%s\":\"bucket_name\"}",
        OVHCloudBucketSchema.BUCKET_NAME); // bucketName property is missing

    assertThrows(JsonMappingException.class, () -> {
      mapper.readValue(jsonString, OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenRegionPropertyIsNotString() {
    String jsonString = String.format("{\"%s\":12, \"%s\":\"bucket_name\"}",
        OVHCloudBucketSchema.REGION, OVHCloudBucketSchema.BUCKET_NAME);

    assertThrows(JsonParseException.class, () -> {
      mapper.readValue("invalid json", OVHCloudS3BucketProvisionedResource.class);
    });
  }

  @Test
  void deserializeShouldThrowErrorWhenBucketPropertyIsNotString() {
    String jsonString = String.format("{\"%s\":\"%s\", \"%s\":12}", OVHCloudBucketSchema.REGION,
        OVHCloudRegions.DEFAULT_REGION, OVHCloudBucketSchema.BUCKET_NAME);

    assertThrows(JsonParseException.class, () -> {
      mapper.readValue("invalid json", OVHCloudS3BucketProvisionedResource.class);
    });
  }
}
