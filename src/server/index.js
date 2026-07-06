import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
async function main() {
    console.log("Starting Peril server...");
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("Connected was successful");
    await declareAndBind(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable);
    console.log("Declared and bound queue for game logs.");
    const channel = await conn.createConfirmChannel();
    publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true })
        .then(() => {
        console.log("Message published successfully");
    })
        .catch((err) => {
        console.error("Error publishing message:", err);
    });
    printServerHelp();
    while (true) {
        const words = await getInput();
        if (words.length === 0)
            continue;
        if (words[0] === "pause") {
            console.log("Pausing the game...");
            publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true })
                .then(() => {
                console.log("Game paused successfully");
            })
                .catch((err) => {
                console.error("Error pausing the game:", err);
            });
        }
        else if (words[0] === "resume") {
            console.log("Resuming the game...");
            publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: false })
                .then(() => {
                console.log("Game resumed successfully");
            })
                .catch((err) => {
                console.error("Error resuming the game:", err);
            });
        }
        else if (words[0] === "quit") {
            console.log("Quitting the server...");
            break;
        }
        else {
            console.log(`Unknown command: ${words[0]}`);
            printServerHelp();
        }
    }
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
