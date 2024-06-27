package com.ovhcloud.edc.extension.s3.api;

import org.eclipse.edc.runtime.metamodel.annotation.ExtensionPoint;
import org.eclipse.edc.spi.EdcException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * This interface provides methods for interacting with OVHcloud S3 buckets.
 */
@ExtensionPoint
public interface S3ConnectorAPI {

  /**
   * Checks if a bucket with the given name exists.
   *
   * @param bucketName the name of the bucket
   * @return true if the bucket exists, false otherwise
   * @throws EdcException if there is an error checking if the bucket exists
   */
  boolean bucketExists(String bucketName) throws EdcException;

  /**
   * Creates a new bucket with the given name. If the bucket already exists, this method does
   * nothing.
   *
   * @param bucketName the name of the bucket to create
   * @throws EdcException if there is an error creating the bucket
   */
  void createBucket(String bucketName) throws EdcException;

  /**
   * Deletes a bucket with the given name. If the bucket does not exist, this method does nothing.
   *
   * @param bucketName the name of the bucket to delete
   * @throws EdcException if there is an error deleting the bucket
   */
  void deleteBucket(String bucketName) throws EdcException;

  /**
   * Lists the objects in a bucket that match a given prefix.
   *
   * @param bucketName the name of the bucket
   * @param prefix     the prefix to match against object names
   * @return a list of object names that match the prefix
   * @throws EdcException if there is an error listing the objects in the bucket
   */
  List<String> listObjects(String bucketName, String prefix) throws EdcException;

  /**
   * Retrieves an object from a bucket.
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object to retrieve
   * @return a ByteArrayInputStream representing the object data
   * @throws EdcException if there is an error retrieving the object from the bucket
   */
  ByteArrayInputStream getObject(String bucketName, String objectName) throws EdcException;

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
  ByteArrayInputStream getObject(String bucketName, String objectName, long offset, long size)
      throws EdcException;

  /**
   * Delete objects from a bucket
   *
   * @param bucketName the name of the bucket
   * @param keys       keys to be deleted
   * @throws EdcException
   */
  void deleteObjects(String bucketName, List<String> keys) throws EdcException;

  /**
   * Put an object into a bucket
   *
   * @param bucketName  the name of the bucket
   * @param objectName  the name of the object
   * @param inputStream the input stream to read the object data from
   */
  void putObject(String bucketName, String objectName, InputStream inputStream);

  /**
   * Get the size of an object in a bucket
   *
   * @param bucketName the name of the bucket
   * @param objectName the name of the object
   * @return the size of the object
   */
  long getObjectSize(String bucketName, String objectName);
}