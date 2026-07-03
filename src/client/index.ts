import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected was successful");
  
  clientWelcome().then(async (username) => {
    console.log(`Client ${username} is ready to send messages.`);
    declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);

    const newGameState = new GameState(username);

    while (true) {
      const words = await getInput();
      if (words.length === 0) continue;

      try {
        if (words[0] === "spawn") {
          commandSpawn(newGameState, words);
        } else if (words[0] === "move") {
          commandMove(newGameState, words);
          console.log("Move command completed successfully.");
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
