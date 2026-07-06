import { ExchangePerilDeadLetter } from "../routing/routing.js";
export var SimpleQueueType;
(function (SimpleQueueType) {
    SimpleQueueType[SimpleQueueType["Durable"] = 0] = "Durable";
    SimpleQueueType[SimpleQueueType["Transient"] = 1] = "Transient";
})(SimpleQueueType = SimpleQueueType || (SimpleQueueType = {}));
export async function declareAndBind(conn, exchange, queueName, key, queueType) {
    const channel = await conn.createChannel();

    const queueOptions = {};
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
export async function subscribeJSON(conn, exchange, queueName, key, queueType, handler) {
    const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    await channel.consume(queue.queue, (message) => {
        if (message === null) {
            return;
        }
        const data = JSON.parse(message.content.toString());
        const shouldAck = handler(data);
        if (shouldAck) {
            channel.ack(message);
        } else {
            channel.nack(message, false, false);
        }
    });
}
