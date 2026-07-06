import amqp, { type Channel } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
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

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => boolean,
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
  );

  await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
    if (message === null) {
      return;
    }

    const data = JSON.parse(message.content.toString()) as T;
    const shouldAck = handler(data);
    if (shouldAck) {
      channel.ack(message);
    } else {
      channel.nack(message, false, false);
    }
  });
}

