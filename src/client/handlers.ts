import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => boolean {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return true;
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => boolean {
    return (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        return outcome !== MoveOutcome.SamePlayer;
    };
}