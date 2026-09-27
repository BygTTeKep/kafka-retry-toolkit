# kafka-retry-toolkit

NestJS-библиотека для работы с ретраями кафки

## Требования

- Node.js 18+
- NestJS 11 или 12

## Установка

```bash
npm install kafka-retry-toolkit
```

Peer-зависимости (если ещё не установлены в проекте):

```bash
npm install @nestjs/common @nestjs/core reflect-metadata rxjs kafkajs @nestjs/microservices
```

## Быстрый старт

### Синхронная конфигурация

```typescript
import { Module } from '@nestjs/common';
import { KafkaRetryModule } from 'kafka-retry-toolkit';

@Module({
  imports: [
    KafkaRetryModule.forRoot({
      isGlobal: true,
      retryDelaysMs: [5000, 60000],
      maxRetries: 2,
      dlqSuffix: '.dlq',
      producer: Producer,
      retryHeader: 'retry-count'
    }),
  ],
})
export class AppModule {}
```

### Асинхронная конфигурация через ConfigModule

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { KafkaRetryModule } from 'kafka-retry-toolkit';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    KafkaRetryModule.forRootAsync({
      isGlobal: true,
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        retryDelaysMs: [5000, 30000, 60000],
        maxRetries: 3,
        dlqSuffix: '.dlq',
        producer: Producer,
        retryHeader: 'retry-count'
      }),
    }),
  ],
})
export class AppModule {}
```


### Параметры модуля

| Параметр    | Тип       | Обязательный | Описание |
|-------------|-----------|--------------|----------|
| `retryDelaysMs`       | `number[]`  | да           | список для топиков с определенной задержкой обработки |
| `maxRetries`    | `number`  | да           | максимальное количество попыток повторной обработки(должно быть >0 и меньше retryDelaysMs.length) |
| `dlqSuffix` | `string`  | да           | строка добавляемая к названию топика в конец(order.process.dlq) |
| `producer`   | `Producer` | да           | Producer из KafkaJS |
| `retryHeader`   | `string` |       да     | заголовок передаваемый в headers сообщения обозначающий текущуюю попытку обработки |
| `isGlobal`  | `boolean` | нет          | Сделать модуль глобальным (по умолчанию `false`) |
