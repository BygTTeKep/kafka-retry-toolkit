import { Producer } from 'kafkajs';

export interface KafkaRetryOptions {
  isGlobal?: boolean;
  retryDelaysMs: number[];
  maxRetries: number;
  dlqSuffix: string;
  producer: Producer;
  retryHeader: string;
}
export interface KafkaRetryAsyncOptions {
  isGlobal?: boolean;
  imports?: any[];
  inject?: any[];
  useFactory: (
    ...args: any[]
  ) => Promise<KafkaRetryOptions> | KafkaRetryOptions;
}
