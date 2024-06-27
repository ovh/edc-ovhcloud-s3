package com.ovhcloud.edc.extension.s3.api;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveBucketArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;

/**
 * This class provides an implementation of the S3ConnectorAPI interface. It uses the MinioClient to
 * interact with OVHcloud S3 buckets.
 */
public class S3ConnectorAPIImpl implements S3ConnectorAPI {

  private final MinioClient s3Client;
  private final Monitor monitor;
  private static final String ERROR_INITIALIZING_S3_CLIENT = "S3 client is not well initialized";

  public S3ConnectorAPIImpl(MinioClient client, Monitor monitor) {
    this.s3Client = client;
    this.monitor = monitor;
  }

  /**
   * Checks if a bucket with the given name exists.
   *
   * @param bucketName the name of the bucket
   * @return true if the bucket exists, false otherwise
   * @throws EdcException if there is an error checking if the bucket exists
   */
  @Override
  public boolean bucketExists(String bucketName) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Checking if bucket exists: " + bucketName));

    try {
      BucketExistsArgs args = BucketExistsArgs
          .builder()
          .bucket(bucketName)
          .build();

      var value = this.s3Client.bucketExists(args);
      return value;
    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException("Error checking if bucket exists: " + e.getMessage());
    }
  }

  /**
   * Creates a new bucket with the given name. If the bucket already exists, this method does
   * nothing.
   *
   * @param bucketName the name of the bucket to create
   * @throws EdcException if there is an error creating the bucket
   */
  @Override
  public void createBucket(String bucketName) throws EdcException {
    checkS3ClientInitialized();

    if (this.bucketExists(bucketName)) {
      Optional.ofNullable(this.monitor)
          .ifPresent(m -> m.debug(
              "bucket " + bucketName + " already exists. Skipping creation."));
      return;
    }

    try {
      Optional.ofNullable(this.monitor)
          .ifPresent(m -> m.debug("Creating bucket: " + bucketName));

      var makeBucketArgs = MakeBucketArgs
          .builder()
          .bucket(bucketName)
          .build();
      this.s3Client.makeBucket(makeBucketArgs);
    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException("Error creating bucket: " + e.getMessage());
    }
  }

  /**
   * Deletes a bucket with the given name. If the bucket does not exist, this method does nothing.
   *
   * @param bucketName the name of the bucket to delete
   * @throws EdcException if there is an error deleting the bucket
   */
  public void deleteBucket(String bucketName) throws EdcException {
    checkS3ClientInitialized();

    if (!this.bucketExists(bucketName)) {
      Optional.ofNullable(this.monitor)
          .ifPresent(m -> m.debug(
              "bucket " + bucketName + " does not exist. Skipping deletion."));
      return;
    }

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Deleting bucket: " + bucketName));

    try {
      var removeBucketArgs = RemoveBucketArgs
          .builder()
          .bucket(bucketName)
          .build();
      this.s3Client.removeBucket(removeBucketArgs);
    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException("Error deleting bucket: " + e.getMessage());
    }
  }

  /**
   * Lists the objects in a bucket that match a given prefix.
   *
   * @param bucketName the name of the bucket
   * @param prefix     the prefix to match against object names
   * @return a list of object names that match the prefix
   * @throws EdcException if there is an error listing the objects in the bucket
   */
  @Override
  public List<String> listObjects(String bucketName, String prefix) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug(
            "Listing bucket objects with prefix: " + prefix + " in bucket: " + bucketName));

    var listObjectsArgs = ListObjectsArgs
        .builder()
        .bucket(bucketName)
        .prefix(prefix)
        .recursive(true)
        .build();

    return StreamSupport.stream(this.s3Client.listObjects(listObjectsArgs).spliterator(), false)
        .map(item -> {
          try {
            return item.get();
          } catch (MinioException | IOException | InvalidKeyException |
                   NoSuchAlgorithmException e) {
            throw new EdcException(
                "Error listing bucket objects in bucket " + bucketName + " : " + e.getMessage());
          }
        })
        .map(Item::objectName)
        .toList();
  }

  /**
   * Retrieves an object from a bucket.
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object to retrieve
   * @return a ByteArrayInputStream representing the object data
   * @throws EdcException if there is an error retrieving the object from the bucket
   */
  @Override
  public ByteArrayInputStream getObject(String bucketName, String objectName) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Getting object: " + objectName + " from bucket: " + bucketName));

    try {
      var getObjectArgs = GetObjectArgs
          .builder()
          .bucket(bucketName)
          .object(objectName)
          .build();

      return new ByteArrayInputStream(this.s3Client.getObject(getObjectArgs).readAllBytes());

    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException(
          "Error getting bucket objects in bucket " + bucketName + " : " + e.getMessage());
    }
  }

  /**
   * Retrieves an object from a bucket.
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object to retrieve
   * @param offset     the offset in the object to start reading from
   * @param size       the number of bytes to read from the object
   * @return a ByteArrayInputStream representing the object data
   * @throws EdcException if there is an error retrieving the object from the bucket
   */
  @Override
  public ByteArrayInputStream getObject(String bucketName, String objectName, long offset,
      long size) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Getting object: " + objectName + " from bucket: " + bucketName));

    try {
      var getObjectArgs = GetObjectArgs
          .builder()
          .bucket(bucketName)
          .object(objectName)
          .offset(offset)
          .length(size)
          .build();

      return new ByteArrayInputStream(this.s3Client.getObject(getObjectArgs).readAllBytes());

    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException(
          "Error getting bucket objects in bucket " + bucketName + " : " + e.getMessage());
    }
  }


  /**
   * Delete objects from a bucket
   *
   * @param bucketName the name of the bucket
   * @param keys       keys to be deleted
   * @throws EdcException
   */
  @Override
  public void deleteObjects(String bucketName, List<String> keys) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Deleting objects " + keys.size() + " from bucket: " + bucketName));

    try {
      List<DeleteObject> objects = new LinkedList<>();
      for (String key : keys) {
        objects.add(new DeleteObject(key));
      }
      var removeObjectsArgs = RemoveObjectsArgs.builder().bucket(bucketName).objects(objects)
          .build();

      var results = this.s3Client.removeObjects(removeObjectsArgs);
      for (Result<DeleteError> result : results) {
        var error = result.get();
        Optional.ofNullable(this.monitor)
            .ifPresent(m -> m.debug(
                "Failed to delete object " + error.objectName() + ": " + error.message()));
      }

    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException(
          "Error removing objects in bucket " + bucketName + " : " + e.getMessage());
    }
  }

  /**
   * Put an object into a bucket
   *
   * @param bucketName  the name of the bucket
   * @param objectName  the name of the object
   * @param inputStream the input stream to read the object data from
   */
  @Override
  public void putObject(String bucketName, String objectName, InputStream inputStream) {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(m -> m.debug("Putting object: " + objectName + " into bucket: " + bucketName));

    try {
      var putObjectArgs = PutObjectArgs.builder()
          .bucket(bucketName)
          .object(objectName)
          .stream(inputStream, inputStream.available(), -1).build();
      this.s3Client.putObject(putObjectArgs);
    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException(
          "Error putting " + objectName + " in bucket " + bucketName + " : "
              + e.getMessage());
    }
  }

  /**
   * Get the size of an object in a bucket
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object
   * @return the size of the object
   */
  @Override
  public long getObjectSize(String bucketName, String objectName) throws EdcException {
    checkS3ClientInitialized();

    Optional.ofNullable(this.monitor)
        .ifPresent(
            m -> m.debug("Getting object size: " + objectName + " from bucket: " + bucketName));

    try {
      StatObjectArgs statObjectArgs = StatObjectArgs.builder()
          .bucket(bucketName)
          .object(objectName)
          .build();

      return this.s3Client.statObject(statObjectArgs).size();

    } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new EdcException(
          "Error Getting size of " + objectName + " in bucket " + bucketName + " : "
              + e.getMessage());
    }

  }

  private void checkS3ClientInitialized() throws EdcException {
    Optional.ofNullable(s3Client).orElseThrow(() -> new EdcException(ERROR_INITIALIZING_S3_CLIENT));
  }
}
