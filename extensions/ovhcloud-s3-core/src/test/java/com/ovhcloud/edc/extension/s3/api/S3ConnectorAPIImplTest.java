package com.ovhcloud.edc.extension.s3.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import io.minio.messages.Contents;
import io.minio.messages.DeleteError;
import io.minio.messages.ErrorResponse;
import io.minio.messages.Item;
import okhttp3.Headers;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class S3ConnectorAPIImplTest {

  private S3ConnectorAPIImpl s3ConnectorAPI;
  private MinioClient minioClient;
  private Monitor monitor;

  @BeforeEach
  public void setUp() {
    minioClient = Mockito.mock(MinioClient.class);
    monitor = Mockito.mock(Monitor.class);
    s3ConnectorAPI = new S3ConnectorAPIImpl(minioClient, monitor);
  }

  @Test
  public void bucketExistsShouldReturnTrueWhenBucketNameIsValid()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {

    when(minioClient.bucketExists(any())).thenReturn(true);

    assertTrue(s3ConnectorAPI.bucketExists("fake-bucket"));
    verify(minioClient, times(1)).bucketExists(any());
  }

  @Test
  public void bucketExistsShouldReturnFalseWhenBucketNameIsUnknown()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {

    when(minioClient.bucketExists(any())).thenReturn(false);

    assertFalse(s3ConnectorAPI.bucketExists("fake-bucket"));
    verify(minioClient, times(1)).bucketExists(any());
  }

  @ParameterizedTest
  @MethodSource("provideExceptions")
  public void bucketExistsShouldThrowEdcException(Exception exception)
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    doThrow(exception).when(minioClient).bucketExists(any(BucketExistsArgs.class));

    assertThrows(EdcException.class, () -> s3ConnectorAPI.bucketExists("throw-exception"));
    verify(minioClient, times(1)).bucketExists(any(BucketExistsArgs.class));
  }

  @Test
  public void makeBucketShouldNotCreateBucketIfExists()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
    when(minioClient.bucketExists(any())).thenReturn(true);

    s3ConnectorAPI.createBucket("fake-bucket");
    verify(minioClient, times(0)).makeBucket(any(MakeBucketArgs.class));
  }

  @Test
  public void makeBucketShouldCreateBucketIfNotExists()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
    when(minioClient.bucketExists(any())).thenReturn(false);

    s3ConnectorAPI.createBucket("fake-bucket");
    verify(minioClient, times(1)).makeBucket(any(MakeBucketArgs.class));
  }

  @ParameterizedTest
  @MethodSource("provideExceptions")
  public void makeBucketShouldThrowEdcException(Exception exception)
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    when(minioClient.bucketExists(any())).thenReturn(false);
    doThrow(exception).when(minioClient).makeBucket(any(MakeBucketArgs.class));

    assertThrows(EdcException.class, () -> s3ConnectorAPI.createBucket("throw-exception"));
    verify(minioClient, times(1)).makeBucket(any(MakeBucketArgs.class));
  }

  @Test
  public void listObjectsShouldReturnListOfObjects()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
    when(minioClient.listObjects(any())).thenAnswer(invocation -> {
          ListObjectsArgs listObjectsArgs = invocation.getArgument(0);
          return new Iterable<Result<Item>>() {
            @Override
            public Iterator<Result<Item>> iterator() {
              List<Result<Item>> list = new ArrayList<>();
              list.add(new Result<>(new Contents("object1")));
              list.add(new Result<>(new Contents("object2")));
              return list.iterator();
            }
          };
        }
    );

    var objects = s3ConnectorAPI.listObjects("fake-bucket", "fake-prefix");
    assertTrue(objects.contains("object1"));
    assertTrue(objects.contains("object2"));
    verify(minioClient, times(1)).listObjects(any());
  }

  @Test
  public void listObjectsShouldNotThrownExceptionIfClientReturnEmptyListOfObjects()
      throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
    when(minioClient.listObjects(any())).thenAnswer(invocation -> {
      return new Iterable<Result<Item>>() {
        @Override
        public Iterator<Result<Item>> iterator() {
          return Collections.emptyListIterator();
        }
      };
    });

    var objects = s3ConnectorAPI.listObjects("fake-bucket", "fake-prefix");
    assertTrue(objects.isEmpty());
    verify(minioClient, times(1)).listObjects(any());
  }

  @Test
  public void getObjectShouldReturnObjectWhenBucketAndObjectAreValid()
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    String bucketName = "fake-bucket";
    String objectName = "fake-object";
    byte[] objectData = "fake-data".getBytes();

    GetObjectArgs getObjectArgs = GetObjectArgs.builder().bucket(bucketName).object(objectName)
        .build();
    InputStream stream = new ByteArrayInputStream(objectData);

    when(minioClient.getObject(getObjectArgs)).thenAnswer(invocation -> new GetObjectResponse(
        null,
        bucketName,
        "gra",
        objectName,
        stream
    ));

    ByteArrayInputStream result = s3ConnectorAPI.getObject(bucketName, objectName);

    byte[] resultData = result.readAllBytes();
    assertArrayEquals(objectData, resultData);
    verify(minioClient, times(1)).getObject(getObjectArgs);
  }

  @ParameterizedTest
  @MethodSource("provideExceptions")
  public void getObjectShouldThrowEdcExceptionWhenErrorOccurs(Exception exception)
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    String bucketName = "fake-bucket";
    String objectName = "fake-object";

    GetObjectArgs getObjectArgs = GetObjectArgs.builder().bucket(bucketName).object(objectName)
        .build();

    when(minioClient.getObject(getObjectArgs)).thenThrow(exception);

    assertThrows(EdcException.class, () -> s3ConnectorAPI.getObject(bucketName, objectName));
    verify(minioClient, times(1)).getObject(getObjectArgs);
  }

  @Test
  void deleteObjectsWithNonEmptyBucketShouldDeleteObjects() {
    String bucketName = "test-bucket";
    List<String> keys = Arrays.asList("object1", "object2");

    List<Result<DeleteError>> results = new LinkedList<>();
    when(minioClient.removeObjects(any(RemoveObjectsArgs.class))).thenReturn(results);

    assertDoesNotThrow(() -> s3ConnectorAPI.deleteObjects(bucketName, keys));

    verify(minioClient, times(1)).removeObjects(any(RemoveObjectsArgs.class));
    verify(monitor, times(1)).debug("Deleting objects 2 from bucket: test-bucket");
    verifyNoMoreInteractions(minioClient, monitor);
  }

  @Test
  void deleteObjectsWithDeleteError() throws Exception {
    String bucketName = "test-bucket";
    List<String> keys = Arrays.asList("object1", "object2");

    DeleteError deleteError = mock(DeleteError.class);
    when(deleteError.objectName()).thenReturn("object1");
    when(deleteError.message()).thenReturn("Delete error");

    Result<DeleteError> result = mock(Result.class);
    when(result.get()).thenReturn(deleteError);
    List<Result<DeleteError>> results = List.of(result);
    when(minioClient.removeObjects(any(RemoveObjectsArgs.class))).thenReturn(results);

    s3ConnectorAPI.deleteObjects(bucketName, keys);

    verify(minioClient, times(1)).removeObjects(any(RemoveObjectsArgs.class));
    verify(monitor, times(1)).debug("Deleting objects 2 from bucket: test-bucket");
    verify(monitor, times(1)).debug("Failed to delete object object1: Delete error");
  }

  @Test
  void getObjectSizeShouldReturnObjectWhenBucketAndObjectAreValid()
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    String bucketName = "fake-bucket";
    String objectName = "fake-object";
    byte[] objectData = "fake-data".getBytes();

    StatObjectArgs statObjectArgs = StatObjectArgs.builder()
        .bucket(bucketName)
        .object(objectName)
        .build();

    when(minioClient.statObject(statObjectArgs)).thenAnswer(invocation -> new StatObjectResponse(
        Headers.of(
            "Content-Length", String.valueOf(objectData.length),
            "Last-Modified", "Fri, 07 Jun 2024 13:30:45 GMT"
        ),
        bucketName,
        "gra",
        objectName
    ));

    long result = s3ConnectorAPI.getObjectSize(bucketName, objectName);

    assertEquals(objectData.length, result);
    verify(minioClient, times(1)).statObject(statObjectArgs);
  }

  @ParameterizedTest
  @MethodSource("provideExceptions")
  void getObjectSizeShouldThrowEdcExceptionWhenErrorOccurs(Exception exception)
      throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
    String bucketName = "fake-bucket";
    String objectName = "fake-object";

    StatObjectArgs statObjectArgs = StatObjectArgs.builder()
        .bucket(bucketName)
        .object(objectName)
        .build();

    when(minioClient.statObject(statObjectArgs)).thenThrow(exception);

    assertThrows(EdcException.class, () -> s3ConnectorAPI.getObjectSize(bucketName, objectName));
    verify(minioClient, times(1)).statObject(statObjectArgs);
  }

  private static @NotNull Stream<Exception> provideExceptions() {
    return Stream.of(
        new ServerException("Server error", 500, ""),
        new InsufficientDataException("Insufficient data error"),
        new ErrorResponseException(new ErrorResponse(), null, null),
        new IOException("IO error"),
        new InvalidKeyException("Invalid key error"),
        new NoSuchAlgorithmException("No such algorithm error")
    );
  }


}