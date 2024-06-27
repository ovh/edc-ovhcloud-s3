# OVHcloud S3 Core Extension

This module provides an implementation of the S3ConnectorAPI interface for interacting with OVHcloud 
S3 buckets. It includes validators for OVHcloud S3 data addresses and a core extension for 
initializing the S3ConnectorAPI service.

__Note:__ This module requires to have an OVHcloud S3 account and credentials (access key and secret key).

## Prerequisites

- Java 17 or higher
- Gradle

## Installation

To add this module to your project, include the following in your `build.gradle` file:

```groovy
dependencies {
    implementation 'com.ovhcloud.edc:ovhcloud-s3-core:1.0.0'
}
```

## Configuration

To use the OVHcloud S3 Core Extension, you need to provide the following configuration properties:

- `edc.ovhcloud.s3.access.key`: The access key for your OVHcloud S3 account.
- `edc.ovhcloud.s3.secret.key`: The secret key for your OVHcloud S3 account.
- `edc.ovhcloud.s3.endpoint`: The endpoint for your OVHcloud S3 account.

You can provide these properties in your `config.properties` file:

```properties
edc.ovhcloud.s3.access.key=<YOUR OVHCLOUD S3 KEY>
edc.ovhcloud.s3.secret.key=<YOUR OVHCLOUD S3 KEY SECRET>
edc.ovhcloud.s3.endpoint=<YOUR OVHCLOUD S3 ENDPOINT>
```
## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[Apache v2.0](https://choosealicense.com/licenses/apache-2.0/)