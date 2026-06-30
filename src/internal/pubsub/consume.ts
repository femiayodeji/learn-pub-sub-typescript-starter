import amqp, { type Channel } from "amqplib";

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
    if (queueType === SimpleQueueType.Durable) queueOptions['durable'] = true;
    if (queueType === SimpleQueueType.Transient) queueOptions['autoDelete'] = true; queueOptions['exclusive'] = true;
    const new_queue = await channel.assertQueue(queueName, queueOptions);
    await channel.bindQueue(new_queue.queue, exchange, key);
    return [channel, new_queue];
    
}