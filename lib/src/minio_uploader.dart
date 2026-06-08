// ignore_for_file: flutter_style_todos

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:convert/convert.dart';
import 'package:crypto/crypto.dart';
import 'package:tf_minio/minio.dart';
import 'package:tf_minio/models.dart';
import 'package:tf_minio/src/minio_client.dart';
import 'package:tf_minio/src/minio_helpers.dart';
import 'package:tf_minio/src/retry_controller.dart';
import 'package:tf_minio/src/utils.dart';

class MinioUploader implements StreamConsumer<Uint8List> {
  MinioUploader(
    this.minio,
    this.client,
    this.bucket,
    this.object,
    this.partSize,
    this.metadata,
    this.onProgress,
    this.maxRetries,
    this.onRetry,
  );

  final Minio minio;
  final MinioClient client;
  final String bucket;
  final String object;
  final int partSize;
  final Map<String, String> metadata;
  final void Function(int)? onProgress;
  final int? maxRetries;
  final void Function(
    UploadRetryStage stage,
    int attemptCount,
    int maxRetries,
    dynamic error,
  )? onRetry;

  var _partNumber = 1;

  String? _etag;

  // Complete object upload, value is the length of the part.
  final _parts = <CompletedPart, int>{};

  Map<int?, Part>? _oldParts;

  String? _uploadId;

  // The number of bytes uploaded of the current part.
  int? bytesUploaded;

  @override
  Future addStream(Stream<Uint8List> stream) async {
    await for (var chunk in stream) {
      List<int>? md5digest;
      final headers = <String, String>{};
      headers.addAll(metadata);
      headers['Content-Length'] = chunk.length.toString();
      if (!client.enableSHA256) {
        md5digest = md5.convert(chunk).bytes;
        headers['Content-MD5'] = base64.encode(md5digest);
      }

      if (_partNumber == 1 && chunk.length < partSize) {
        _etag = await _uploadChunk(chunk, headers, null, 1);
        return;
      }

      if (_uploadId == null) {
        await _initMultipartUpload();
      }

      final partNumber = _partNumber++;

      if (_oldParts != null) {
        final oldPart = _oldParts![partNumber];
        if (oldPart != null) {
          md5digest ??= md5.convert(chunk).bytes;
          if (hex.encode(md5digest) == oldPart.eTag) {
            final part = CompletedPart(oldPart.eTag, partNumber);
            _parts[part] = oldPart.size!;
            continue;
          }
        }
      }

      final queries = <String, String?>{
        'partNumber': '$partNumber',
        'uploadId': _uploadId,
      };

      final etag = await _uploadChunk(chunk, headers, queries, partNumber);
      final part = CompletedPart(etag, partNumber);
      _parts[part] = chunk.length;
    }
  }

  @override
  Future<String?> close() async {
    if (_uploadId == null) return _etag;
    return AsyncOperation.retry(
      operation: () => minio.completeMultipartUpload(
        bucket,
        object,
        _uploadId!,
        _parts.keys.toList(),
      ),
      retryController: _buildNetworkRetryController(
        stage: UploadRetryStage.completeMultipartUpload,
      ),
    );
  }

  Map<String, String> getHeaders(List<int> chunk) {
    final headers = <String, String>{};
    headers.addAll(metadata);
    headers['Content-Length'] = chunk.length.toString();
    if (!client.enableSHA256) {
      final md5digest = md5.convert(chunk).bytes;
      headers['Content-MD5'] = base64.encode(md5digest);
    }
    return headers;
  }

  Future<String?> _uploadChunk(
    Uint8List chunk,
    Map<String, String> headers,
    Map<String, String?>? queries,
    int? partNumber,
  ) async {
    final resp = await AsyncOperation.retry(
      operation: () async {
        final response = await client.request(
          method: 'PUT',
          headers: headers,
          queries: queries,
          bucket: bucket,
          object: object,
          payload: chunk,
          onProgress: _updateProgress,
        );

        validate(response);
        return response;
      },
      retryController: _buildNetworkRetryController(
        stage: UploadRetryStage.uploadChunk,
        partNumber: partNumber,
      ),
    );

    var etag = resp.headers['etag'];
    if (etag != null) etag = trimDoubleQuote(etag);

    return etag;
  }

  Future<void> _initMultipartUpload() async {
    // FIXME: this code still causes Signature Error
    // FIXME: https://github.com/xtyxtyx/minio-dart/issues/7
    // TODO: uncomment when fixed
    // uploadId = await minio.findUploadId(bucket, object);

    if (_uploadId == null) {
      _uploadId = await AsyncOperation.retry(
        operation: () =>
            minio.initiateNewMultipartUpload(bucket, object, metadata),
        retryController: _buildNetworkRetryController(
          stage: UploadRetryStage.initiateMultipartUpload,
        ),
      );
      return;
    }

    final parts = minio.listParts(bucket, object, _uploadId!);
    final entries = await AsyncOperation.retry(
      operation: () =>
          parts.asyncMap((part) => MapEntry(part.partNumber, part)).toList(),
      retryController: _buildNetworkRetryController(
        stage: UploadRetryStage.listParts,
      ),
    );
    _oldParts = Map.fromEntries(entries);
  }

  RetryController _buildNetworkRetryController({
    required UploadRetryStage stage,
    int? partNumber,
  }) {
    return RetryController.forNetworkErrors(
      maxRetries: maxRetries ?? RetryController.defaultMaxRetries,
      onRetry: (attemptCount, retries, error) {
        onRetry?.call(stage, attemptCount, retries, error);

        final target = partNumber == null
            ? stage.wireValue
            : '${stage.wireValue}(part=$partNumber)';
        print(
          'Retrying $target (attempt $attemptCount/$retries) due to error: $error',
        );
      },
    );
  }

  void _updateProgress(int bytesUploaded) {
    this.bytesUploaded = bytesUploaded;
    _reportUploadProgress();
  }

  void _reportUploadProgress() {
    if (onProgress == null || bytesUploaded == null) {
      return;
    }

    var totalBytesUploaded = bytesUploaded!;

    for (var part in _parts.keys) {
      totalBytesUploaded += _parts[part]!;
    }

    onProgress!(totalBytesUploaded);
  }
}
