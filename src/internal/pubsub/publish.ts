import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const serializedValue = JSON.stringify(value);
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, Buffer.from(serializedValue), { contentType: "application/json" }, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
};

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const serializedValue = Buffer.from(JSON.stringify(value));
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, serializedValue, { contentType: "application/x-msgpack" }, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}