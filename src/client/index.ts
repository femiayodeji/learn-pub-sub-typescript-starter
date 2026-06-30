import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected was successful");
  
  clientWelcome().then((username) => {
    console.log(`Client ${username} is ready to send messages.`);
    declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);

  }).catch((err) => {
    console.error("Error during client welcome:", err);
    process.exit(1);
  });

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
