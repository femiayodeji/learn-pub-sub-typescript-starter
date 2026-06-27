import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  PauseKey,
} from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected was successful");

  const channel = await conn.createConfirmChannel();

  publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true })
    .then(() => {
      console.log("Message published successfully");
    })
    .catch((err) => {
      console.error("Error publishing message:", err);
    });

  process.stdin.resume();
  process.on('SIGINT', () => {
    console.log("Closing connection...");
    conn.close().then(() => {
      console.log("Connection closed");
      process.exit(0);
    }).catch((err) => {
      console.error("Error closing connection:", err);
      process.exit(1);
    });
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
