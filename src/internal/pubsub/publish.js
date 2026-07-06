export function publishJSON(ch, exchange, routingKey, value) {
    const serializedValue = JSON.stringify(value);
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, Buffer.from(serializedValue), { contentType: "application/json" }, (err) => {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
}
;
