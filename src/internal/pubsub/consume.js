import { ExchangePerilDeadLetter } from "../routing/routing.js";
export var SimpleQueueType;
(function (SimpleQueueType) {
    SimpleQueueType[SimpleQueueType["Durable"] = 0] = "Durable";
    SimpleQueueType[SimpleQueueType["Transient"] = 1] = "Transient";
})(SimpleQueueType = SimpleQueueType || (SimpleQueueType = {}));
export var AckType;
(function (AckType) {
    AckType[AckType["Ack"] = 0] = "Ack";
    AckType[AckType["NackDiscard"] = 1] = "NackDiscard";
    AckType[AckType["NackRequeue"] = 2] = "NackRequeue";
})(AckType = AckType || (AckType = {}));
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
    await channel.consume(queue.queue, async (message) => {
        if (message === null) {
            return;
        }
        const data = JSON.parse(message.content.toString());
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
