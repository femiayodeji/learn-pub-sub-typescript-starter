import type { ConfirmChannel } from "amqplib";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { type ArmyMove, type RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        if (outcome === MoveOutcome.MakeWar) {
            const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: gs.getPlayerSnap(),
            };
            try {
                await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`, rw);
                return AckType.Ack;
            } catch {
                return AckType.NackRequeue;
            }
        }
        if (outcome === MoveOutcome.SamePlayer) {
            return AckType.NackDiscard;
        }
        return AckType.Ack;
    };
}

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar) => {
        const resolution = handleWar(gs, rw);
        process.stdout.write("> ");
        switch (resolution.result) {
            case WarOutcome.NotInvolved:
                return AckType.NackRequeue;
            case WarOutcome.NoUnits:
                return AckType.NackDiscard;
            case WarOutcome.OpponentWon:
                return AckType.Ack;
            case WarOutcome.YouWon:
                return AckType.Ack;
            case WarOutcome.Draw:
                return AckType.Ack;
            default:
                console.error("Unknown war outcome");
                return AckType.NackDiscard;
        }
    };
}