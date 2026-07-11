import amqp, { type Channel } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackDiscard,
  NackRequeue,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const channel = await conn.createChannel();

    const queueOptions: amqp.Options.AssertQueue = {};
    if (queueType === SimpleQueueType.Durable) 
      queueOptions['durable'] = true;
    if (queueType === SimpleQueueType.Transient) {
      queueOptions['autoDelete'] = true;
    }
    queueOptions['arguments'] = { "x-dead-letter-exchange": ExchangePerilDeadLetter };
    const new_queue = await channel.assertQueue(queueName, queueOptions);
    await channel.bindQueue(new_queue.queue, exchange, key);
    return [channel, new_queue];
    
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
  );
  await channel.prefetch(10);

  await channel.consume(queue.queue, async (message: amqp.ConsumeMessage | null) => {
    if (message === null) {
      return;
    }

    const data = deserializer(message.content);
    const ackType = await handler(data);
    switch (ackType) {
      case AckType.Ack:
        channel.ack(message);
        break;
      case AckType.NackDiscard:
        channel.nack(message, false, false);
        break;
      case AckType.NackRequeue:
        channel.nack(message, false, true);
        break;
    }
  });
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => JSON.parse(data.toString()) as T,
  );
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
    handler,
    (data: Buffer) => decode(data) as T,
  );
}

