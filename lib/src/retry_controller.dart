import 'dart:async';
import 'dart:io';
import 'dart:math';

/// 重试策略类型
enum RetryStrategy {
  /// 固定延迟重试
  fixedDelay,

  /// 指数退避重试
  exponentialBackoff,

  /// 随机指数退避重试（推荐）
  jitteredExponentialBackoff,

  /// 立即重试
  immediate,
}

/// 重试条件类型
typedef RetryCondition = bool Function(
  Object error,
  StackTrace? stackTrace,
  int attemptCount,
);

/// 重试控制器
class RetryController {
  /// 重试控制器构造函数
  RetryController({
    this.strategy = RetryStrategy.jitteredExponentialBackoff,
    this.maxRetries = defaultMaxRetries,
    this.initialDelay = defaultInitialDelay,
    this.maxDelay = defaultMaxDelay,
    this.retryCondition,
    this.onRetry,
    this.onSuccess,
    this.onFailure,
    this.enableJitter = true,
  })  : assert(maxRetries >= 0, 'maxRetries must be non-negative'),
        assert(initialDelay >= 0, 'initialDelay must be non-negative'),
        assert(maxDelay >= initialDelay, 'maxDelay must be >= initialDelay');

  /// 默认最大重试次数
  static const int defaultMaxRetries = 3;

  /// 默认初始延迟（毫秒）
  static const int defaultInitialDelay = 1000;

  /// 默认最大延迟（毫秒）
  static const int defaultMaxDelay = 30000;

  /// 重试策略
  final RetryStrategy strategy;

  /// 最大重试次数
  final int maxRetries;

  /// 初始延迟（毫秒）
  final int initialDelay;

  /// 最大延迟（毫秒）
  final int maxDelay;

  /// 重试条件
  final RetryCondition? retryCondition;

  /// 重试前回调
  final Function(int attemptCount, int maxRetries, Object? error)? onRetry;

  /// 重试成功回调
  final Function(int attemptCount)? onSuccess;

  /// 重试失败回调
  final Function(Object error, StackTrace? stackTrace, int attemptCount)?
      onFailure;

  /// 是否启用抖动（随机性）
  final bool enableJitter;

  /// 执行带重试的异步操作
  Future<T> execute<T>(Future<T> Function() operation) async {
    int attemptCount = 0;
    Object? lastError;
    StackTrace? lastStackTrace;

    while (attemptCount <= maxRetries) {
      attemptCount++;

      try {
        final result = await operation();

        // 操作成功
        onSuccess?.call(attemptCount);
        return result;
      } catch (error, stackTrace) {
        lastError = error;
        lastStackTrace = stackTrace;

        // 检查是否应该重试
        if (!_shouldRetry(error, stackTrace, attemptCount)) {
          onFailure?.call(error, stackTrace, attemptCount);
          rethrow;
        }

        // 如果是最后一次尝试，不再等待直接抛出
        if (attemptCount > maxRetries) {
          onFailure?.call(error, stackTrace, attemptCount);
          rethrow;
        }

        // 计算延迟时间
        final delay = _calculateDelay(attemptCount);

        // 调用重试回调
        onRetry?.call(attemptCount, maxRetries, error);

        // 等待延迟时间
        await Future.delayed(Duration(milliseconds: delay));
      }
    }

    // 理论上不会到达这里，但为了编译器安全
    onFailure?.call(lastError!, lastStackTrace, attemptCount);
    throw lastError!;
  }

  /// 检查是否应该重试
  bool _shouldRetry(Object error, StackTrace? stackTrace, int attemptCount) {
    // 如果提供了自定义重试条件，使用它
    if (retryCondition != null) {
      return retryCondition!(error, stackTrace, attemptCount);
    }

    // 默认重试条件：重试次数未达到最大值
    return attemptCount <= maxRetries;
  }

  /// 计算延迟时间（毫秒）
  int _calculateDelay(int attemptCount) {
    switch (strategy) {
      case RetryStrategy.fixedDelay:
        return initialDelay;

      case RetryStrategy.immediate:
        return 0;

      case RetryStrategy.exponentialBackoff:
        return _calculateExponentialDelay(attemptCount, false);

      case RetryStrategy.jitteredExponentialBackoff:
        return _calculateExponentialDelay(attemptCount, true);
    }
  }

  /// 计算指数退避延迟
  int _calculateExponentialDelay(int attemptCount, bool withJitter) {
    // 指数退避公式：initialDelay * (2^(attemptCount-1))
    var delay = initialDelay * pow(2, attemptCount - 1).toInt();

    // 应用抖动（随机性）
    if (withJitter && enableJitter) {
      final randomFactor = 0.5 + Random().nextDouble(); // 0.5 to 1.5
      delay = (delay * randomFactor).toInt();
    }

    // 限制最大延迟
    return min(delay, maxDelay);
  }

  /// 创建针对网络错误的重试控制器
  static RetryController forNetworkErrors({
    int maxRetries = 5,
    RetryStrategy strategy = RetryStrategy.jitteredExponentialBackoff,
    int initialDelay = 1000,
    int maxDelay = 60000,
    bool enableHttpRequestRetry = true,
    bool enableSocketRetry = true,
    bool enableTimeoutRetry = true,
    Function(int attemptCount, int maxRetries, Object? error)? onRetry,
    Function(int attemptCount)? onSuccess,
    Function(Object error, StackTrace? stackTrace, int attemptCount)? onFailure,
  }) {
    return RetryController(
      strategy: strategy,
      maxRetries: maxRetries,
      initialDelay: initialDelay,
      maxDelay: maxDelay,
      retryCondition: (error, stackTrace, attemptCount) {
        // 检查是否是网络相关的错误
        if (enableHttpRequestRetry && _isHttpRequestError(error)) {
          return true;
        }

        if (enableSocketRetry && _isSocketError(error)) {
          return true;
        }

        if (enableTimeoutRetry && _isTimeoutError(error)) {
          return true;
        }

        // 也可以添加其他特定的错误类型
        return false;
      },
      onRetry: onRetry,
      onSuccess: onSuccess,
      onFailure: onFailure,
    );
  }

  /// 创建针对数据库操作的重试控制器
  static RetryController forDatabaseOperations({
    int maxRetries = 3,
    RetryStrategy strategy = RetryStrategy.exponentialBackoff,
    int initialDelay = 500,
    int maxDelay = 10000,
    bool enableDeadlockRetry = true,
    bool enableConnectionRetry = true,
    bool enableTimeoutRetry = true,
    Function(int attemptCount, int maxRetries, Object? error)? onRetry,
    Function(int attemptCount)? onSuccess,
    Function(Object error, StackTrace? stackTrace, int attemptCount)? onFailure,
  }) {
    return RetryController(
      strategy: strategy,
      maxRetries: maxRetries,
      initialDelay: initialDelay,
      maxDelay: maxDelay,
      retryCondition: (error, stackTrace, attemptCount) {
        final errorString = error.toString().toLowerCase();

        if (enableDeadlockRetry && errorString.contains('deadlock')) {
          return true;
        }

        if (enableConnectionRetry &&
            (errorString.contains('connection') ||
                errorString.contains('connectivity'))) {
          return true;
        }

        if (enableTimeoutRetry && errorString.contains('timeout')) {
          return true;
        }

        return false;
      },
      onRetry: onRetry,
      onSuccess: onSuccess,
      onFailure: onFailure,
    );
  }

  /// 检查是否是HTTP请求错误
  static bool _isHttpRequestError(Object error) {
    final errorString = error.toString().toLowerCase();
    return errorString.contains('http') ||
        errorString.contains('status code') ||
        errorString.contains('429') || // Too Many Requests
        errorString.contains('500') || // Internal Server Error
        errorString.contains('502') || // Bad Gateway
        errorString.contains('503') || // Service Unavailable
        errorString.contains('504'); // Gateway Timeout
  }

  /// 检查是否是Socket错误
  static bool _isSocketError(Object error) {
    return error is SocketException ||
        error.toString().toLowerCase().contains('socket') ||
        error.toString().toLowerCase().contains('connection reset') ||
        error.toString().toLowerCase().contains('broken pipe');
  }

  /// 检查是否是超时错误
  static bool _isTimeoutError(Object error) {
    return error is TimeoutException ||
        error.toString().toLowerCase().contains('timeout') ||
        error.toString().toLowerCase().contains('timed out');
  }
}

/// 异步操作包装器
class AsyncOperation {
  /// 带重试的异步操作
  static Future<T> retry<T>({
    required Future<T> Function() operation,
    RetryController? retryController,
    int? maxRetries,
    RetryStrategy? strategy,
    int? initialDelay,
    int? maxDelay,
    RetryCondition? retryCondition,
    Function(int attemptCount, int maxRetries, Object? error)? onRetry,
    Function(int attemptCount)? onSuccess,
    Function(Object error, StackTrace? stackTrace, int attemptCount)? onFailure,
  }) async {
    final controller = retryController ??
        RetryController(
          maxRetries: maxRetries ?? RetryController.defaultMaxRetries,
          strategy: strategy ?? RetryStrategy.jitteredExponentialBackoff,
          initialDelay: initialDelay ?? RetryController.defaultInitialDelay,
          maxDelay: maxDelay ?? RetryController.defaultMaxDelay,
          retryCondition: retryCondition,
          onRetry: onRetry,
          onSuccess: onSuccess,
          onFailure: onFailure,
        );

    return controller.execute(operation);
  }

  /// 带超时的异步操作
  static Future<T> withTimeout<T>({
    required Future<T> Function() operation,
    required Duration timeout,
    T? fallbackValue,
    Function(Object error, StackTrace stackTrace)? onError,
  }) async {
    try {
      return await operation().timeout(
        timeout,
        onTimeout: fallbackValue == null ? null : () => fallbackValue as T,
      );
    } catch (error, stackTrace) {
      if (error is TimeoutException) {
        if (fallbackValue != null) {
          return fallbackValue as T;
        }
      }

      onError?.call(error, stackTrace);
      rethrow;
    }
  }

  /// 带重试和超时的异步操作
  static Future<T> retryWithTimeout<T>({
    required Future<T> Function() operation,
    RetryController? retryController,
    Duration? timeoutPerAttempt,
    Duration? totalTimeout,
    T? fallbackValue,
  }) async {
    final stopwatch = Stopwatch()..start();

    return retry(
      operation: () async {
        if (totalTimeout != null && stopwatch.elapsed >= totalTimeout) {
          throw TimeoutException('Total timeout exceeded');
        }

        if (timeoutPerAttempt != null) {
          return await withTimeout(
            operation: operation,
            timeout: timeoutPerAttempt,
            fallbackValue: fallbackValue,
          );
        }

        return await operation();
      },
      retryController: retryController,
    );
  }
}

/// 取消令牌
class CancellationToken {
  bool _isCancelled = false;
  final List<VoidCallback> _listeners = [];

  /// 检查是否已取消
  bool get isCancelled => _isCancelled;

  /// 取消操作
  void cancel() {
    if (_isCancelled) return;
    _isCancelled = true;
    for (final listener in _listeners) {
      listener();
    }
    _listeners.clear();
  }

  /// 添加取消监听器
  void addListener(VoidCallback listener) {
    if (_isCancelled) {
      listener();
    } else {
      _listeners.add(listener);
    }
  }

  /// 移除取消监听器
  void removeListener(VoidCallback listener) {
    _listeners.remove(listener);
  }

  /// 确保未取消，如果已取消则抛出异常
  void throwIfCancelled() {
    if (_isCancelled) {
      throw OperationCanceledException();
    }
  }
}

/// 操作取消异常
class OperationCanceledException implements Exception {
  @override
  String toString() => 'Operation was canceled';
}

typedef VoidCallback = void Function();
