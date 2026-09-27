import { Inject, Injectable } from '@nestjs/common';
import { KAFKA_RETRY_OPTIONS } from '../constants';
import type { KafkaRetryOptions } from '../interfaces/kafka-retry-options.interface';

@Injectable()
export class RetryTopicResolverService {
  constructor(
    @Inject(KAFKA_RETRY_OPTIONS)
    private readonly retryOptions: KafkaRetryOptions,
  ) {}
  getRetryTopic(originalTopic: string, retryCount: number) {
    const delays = this.retryOptions.retryDelaysMs;
    const index = retryCount - 1;
    if (index < 0 || index >= delays.length) {
      throw new Error(`No retry topic configured for retry #${retryCount}`);
    }

    const label = this.formatDelayLabel(delays[index]);
    return `${originalTopic}.retry.${label}`;
  }
  getDlqTopic(originalTopic: string): string {
    return `${originalTopic}${this.retryOptions.dlqSuffix}`;
  }

  private formatDelayLabel(ms: number): string {
    if (ms % 60000 === 0) return `${ms / 60000}m`;
    return `${ms / 1000}s`;
  }
}
