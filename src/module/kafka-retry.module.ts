import { DynamicModule, Module } from '@nestjs/common';
import {
  KafkaRetryAsyncOptions,
  KafkaRetryOptions,
} from '../interfaces/kafka-retry-options.interface';
import { KAFKA_RETRY_OPTIONS } from '../constants';
import { KafkaRetryExceptionFilter } from '../filters/kafka-retry-exception.filter';
import { RetryTopicResolverService } from '../services/retry-topic-resolver.service';

@Module({})
export class KafkaRetryModule {
  static forRoot(options: KafkaRetryOptions): DynamicModule {
    return {
      module: KafkaRetryModule,
      global: options.isGlobal ?? false,
      providers: [
        { provide: KAFKA_RETRY_OPTIONS, useValue: options },
        KafkaRetryExceptionFilter,
        RetryTopicResolverService,
      ],
      exports: [KafkaRetryExceptionFilter, RetryTopicResolverService],
    };
  }
  static forRootAsync(options: KafkaRetryAsyncOptions): DynamicModule {
    return {
      module: KafkaRetryModule,
      global: options.isGlobal ?? false,
      imports: options.imports ?? [],
      providers: [
        {
          provide: KAFKA_RETRY_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        KafkaRetryExceptionFilter,
        RetryTopicResolverService,
      ],
      exports: [KafkaRetryExceptionFilter, RetryTopicResolverService],
    };
  }
  /**
   * Возвращает полный список топиков (оригинальный + все retry + dlq),
   * на которые нужно подписать consumer, чтобы фильтр работал корректно.
   */
  static getAllTopicsForSubscription(
    originalTopic: string,
    options: Pick<KafkaRetryOptions, 'retryDelaysMs' | 'dlqSuffix'>,
  ): string[] {
    const resolver = new RetryTopicResolverService(
      options as KafkaRetryOptions,
    );

    const retryTopics = options.retryDelaysMs.map((_, idx) =>
      resolver.getRetryTopic(originalTopic, idx + 1),
    );

    const dlqTopic = resolver.getDlqTopic(originalTopic);

    return [originalTopic, ...retryTopics, dlqTopic];
  }
}
