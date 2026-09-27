import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  Inject,
  Logger,
} from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { RetryTopicResolverService } from '../services/retry-topic-resolver.service';
import type { KafkaRetryOptions } from '../interfaces/kafka-retry-options.interface';
import { KAFKA_RETRY_OPTIONS, ORIGINAL_TOPIC_HEADER } from '../constants';

@Catch()
export class KafkaRetryExceptionFilter implements ExceptionFilter {
  private readonly ORIGINAL_TOPIC_HEADER = ORIGINAL_TOPIC_HEADER;
  private readonly logger = new Logger(KafkaRetryExceptionFilter.name);
  constructor(
    @Inject(KAFKA_RETRY_OPTIONS)
    private readonly retryOptions: KafkaRetryOptions,
    private readonly topicResolverService: RetryTopicResolverService,
  ) {}
  async catch(exception: any, host: ArgumentsHost) {
    const kafkaContext = host.switchToRpc().getContext<KafkaContext>();
    const countRetries = this.getCountRetryFromContext(kafkaContext);
    if (countRetries >= this.retryOptions.maxRetries) {
      //Кидаем в DLQ очередь
      try {
        await this.publishToDLQ(kafkaContext, exception);
        await this.commitOffset(kafkaContext);
      } catch (err) {
        this.logger.error(`Failed public to DLQ or commit: ${err}`);
      }
      return;
    }

    try {
      await this.republishWithRetry(kafkaContext, countRetries + 1);
      await this.commitOffset(kafkaContext);
    } catch (err) {
      this.logger.error(`Failed to republic or commit offset: ${err}`);
      throw exception;
    }
  }

  private getCountRetryFromContext(ctx: KafkaContext): number {
    const headers = ctx.getMessage().headers || {};
    const retryHeader = headers[this.retryOptions.retryHeader];
    if (!retryHeader) return 0;

    const val = Buffer.isBuffer(retryHeader)
      ? retryHeader.toString()
      : String(retryHeader);
    return parseInt(val, 10) || 0;
  }
  private async republishWithRetry(ctx: KafkaContext, retryCount: number) {
    const originalTopic = this.getOriginalTopic(ctx);
    const nextTopic = this.topicResolverService.getRetryTopic(
      originalTopic,
      retryCount,
    );

    const message = ctx.getMessage();

    await this.retryOptions.producer.send({
      topic: nextTopic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            [this.retryOptions.retryHeader]: retryCount.toString(),
            [this.ORIGINAL_TOPIC_HEADER]: originalTopic,
          },
        },
      ],
    });
  }
  private async commitOffset(ctx: KafkaContext) {
    const consumer = ctx.getConsumer();
    if (!consumer) {
      throw new Error('Consumer instance is not available from KafkaContext.');
    }

    const topic = ctx.getTopic();
    const partition = ctx.getPartition();
    const message = ctx.getMessage();
    const offset = message.offset;

    if (!topic || partition === undefined || offset === undefined) {
      throw new Error(
        'Incomplete Kafka message context for committing offset.',
      );
    }

    await consumer.commitOffsets([
      {
        topic,
        partition,
        offset: (Number(offset) + 1).toString(),
      },
    ]);
  }
  private async publishToDLQ(ctx: KafkaContext, exception: any) {
    const originalTopic = this.getOriginalTopic(ctx);
    const message = ctx.getMessage(); //TODO переформировать message с понятной ошибкой
    await this.retryOptions.producer.send({
      topic: this.topicResolverService.getDlqTopic(originalTopic),
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            [this.ORIGINAL_TOPIC_HEADER]: originalTopic,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            'x-failure-reason': String(exception?.message ?? 'unknown'),
          },
        },
      ],
    });
  }
  private getOriginalTopic(ctx: KafkaContext): string {
    const headers = ctx.getMessage().headers || {};
    const header = headers[this.ORIGINAL_TOPIC_HEADER];
    if (header) {
      return Buffer.isBuffer(header) ? header.toString() : String(header);
    }
    // если заголовка нет - значит это первая ошибка, топик и есть оригинальный
    return ctx.getTopic();
  }
}
