import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
export function handlerPause(gs) {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return true;
    };
}
export function handlerMove(gs) {
    return (move) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        return outcome !== MoveOutcome.SamePlayer;
    };
}
