import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerMove, handlerPause } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected was successful");
  
  clientWelcome().then(async (username) => {
    console.log(`Client ${username} is ready to send messages.`);

    const newGameState = new GameState(username);
    const moveRoutingKey = `${ArmyMovesPrefix}.${username}`;

    await subscribeJSON(
      conn,
      ExchangePerilDirect,
      `pause.${username}`,
      PauseKey,
      SimpleQueueType.Transient,
      handlerPause(newGameState),
    );

    await subscribeJSON(
      conn,
      ExchangePerilTopic,
      moveRoutingKey,
      `${ArmyMovesPrefix}.*`,
      SimpleQueueType.Transient,
      handlerMove(newGameState),
    );

    const moveChannel = await conn.createConfirmChannel();

    while (true) {
      const words = await getInput();
      if (words.length === 0) continue;

      try {
        if (words[0] === "spawn") {
          commandSpawn(newGameState, words);
        } else if (words[0] === "move") {
          const move = commandMove(newGameState, words);
          await publishJSON(moveChannel, ExchangePerilTopic, moveRoutingKey, move);
          console.log("Move published successfully.");
        } else if (words[0] === "status") { 
          commandStatus(newGameState);
        } else if (words[0] === "help") {
          printClientHelp();
        } else if (words[0] === "spam") {
          console.log("Spamming not allowed yet!");
        } else if (words[0] === "quit") {
          printQuit();
          process.exit(0);
        } else {
          console.log(`Unknown command: ${words[0]}`);
          printClientHelp();
        }
      } catch (error) {
        if (error instanceof Error) {
          console.error(error.message);
        } else {
          console.error("An unknown error occurred.");
        }
      }
    }
    
  }).catch((err) => {
    console.error("Error during client welcome:", err);
    process.exit(1);
  });

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
