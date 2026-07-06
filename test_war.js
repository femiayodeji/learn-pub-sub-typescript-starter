import amqp from "amqplib";

async function main() {
  const conn = await amqp.connect("amqp://guest:guest@localhost:5672/");

  // Create the war queue with DLX
  const ch = await conn.createChannel();
  await ch.assertExchange("peril_dlx", "fanout");
  await ch.assertQueue("peril_dlq");
  await ch.bindQueue("peril_dlq", "peril_dlx", "");
  await ch.assertExchange("peril_topic", "topic");

  await ch.assertQueue("war", { durable: true, arguments: { "x-dead-letter-exchange": "peril_dlx" } });
  await ch.bindQueue("war", "peril_topic", "war.*");

  // Send a RecognitionOfWar message to the war queue
  const rw = {
    attacker: { username: "washington", units: {} },
    defender: { username: "napoleon", units: {} },
  };

  // Publish many war messages to trigger redeliveries
  const confirmCh = await conn.createConfirmChannel();
  for (let i = 0; i < 10; i++) {
    confirmCh.publish("peril_topic", "war.washington", Buffer.from(JSON.stringify(rw)));
  }
  await confirmCh.waitForConfirms();

  // Now consume and nack-requeue to create redeliveries
  const consumerCh = await conn.createChannel();
  await consumerCh.consume("war", async (msg) => {
    if (msg) {
      // Nack with requeue to create redeliveries
      consumerCh.nack(msg, false, true);
    }
  });

  console.log("Madness started! Press Ctrl+C to stop.");
}

main().catch(console.error);
