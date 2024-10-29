import csv
import os
from typing import List

import tqdm

from LogInterface import Log
from Primitive import *
from StreamUtils import *
from Utils import (
    ObservationJosh,
    countLines,
    displayTopMemoryConsumers,
    extractTrajNumbers,
    profileFunction,
    readLastLine,
    startMemoryTracing,
)


def checkPointCallback(cnt):
    if cnt % 1000 == 0:
        print(f"Checkpoint: {cnt}")
        # snapshot = tracemalloc.take_snapshot()
        # displayTopMemoryConsumers(snapshot)


def main():
    startMemoryTracing()
    LOG = Log()
    # LOG.readLogFile("bc18_adam.log")
    # LOG.readLogFile("Reference.log")
    LOG.readLogFile("1.log")
    # LOG.eval()
    LOG.eval(isLogFileLarge=False)

    # Dump all the representations into json and jpg images
    LOG.parseBytes()
    # for frame in tqdm.tqdm(LOG.frames):
    for frame in LOG.frames:
        # if "Stopwatch" not in frame:
        #     continue
        # timeCost = frame["Stopwatch"]["AllModules"]
        # if timeCost>1000:
        #     print(timeCost, frame.timestamp)
        # if frame.hasImage:
        #     frame.saveImageWithMetaData()
        frame.saveFrameDict()
        frame.saveImageWithMetaData(slientFail=True)
    return

    # Sample of how I recover trajectories
    OBS = ObservationJosh("WalkToBall")
    index = 0

    collisions = []
    prev_state = None
    prevObs = None
    transitions: List[List] = []

    lastCsvFrame = 0
    lastTrajFrame = 0

    csvLine = 0
    if LOG.outputDir.exists():
        csvLine = countLines(LOG.outputDir / "data.csv")

        if csvLine == 1:  # Only title line
            with open(LOG.outputDir / "data.csv", "w") as f:
                print("Only title line found in csv, recreate it")
                pass
            csvLine = 0

        if csvLine != 0:
            lastCsvLine = readLastLine(LOG.outputDir / "data.csv").split(",")
            lastCsvFrame = int(lastCsvLine[0])

        collectedTrajs = extractTrajNumbers(LOG.outputDir)
        if len(collectedTrajs) != 0:
            lastTrajFrame = collectedTrajs[-1]

    print(f"Last csv frame: {lastCsvFrame}")

    print(f"Last traj frame: {lastTrajFrame}")

    ContinuePos = min(lastCsvFrame, lastTrajFrame)

    print(f"Continue from {ContinuePos}")

    if not (LOG.outputDir / "data.csv").exists():
        os.makedirs(LOG.outputDir, exist_ok=True)
        with open(LOG.outputDir / "data.csv", "w") as f:
            print("Create csv")
            pass

    with open(LOG.outputDir / "data.csv", "a") as f:
        writer = csv.writer(f)
        if csvLine == 0:
            writer.writerow(
                [
                    "frameIndex",
                    "timestamp",
                    "agentLoc",
                    "ballLoc",
                    *[
                        "robotSpeedX",
                        "robotSpeedY",
                        "robotSpeedRot",
                        "kickType",
                        "kickLength",
                        "alignPreciselyModified",
                        "rollOutResult",
                    ],
                    "jsonFile",
                ]
            )
        prevAgentLoc = None
        prevBallLoc = None

        GAME_STATE = LOG.TypeInfoChunk.enumClasses["GameState::State"]

        for frame in LOG.UncompressedChunk.threads["Cognition"]:
            if frame.absIndex < ContinuePos:
                continue
            checkPointCallback(frame.indexCursor)
            print(f"Frame {frame.indexCursor}")

            try:
                gameState = frame["GameState"]
                playerNumber = gameState["playerNumber"]

                state = gameState["state"]

                if state == GAME_STATE["playing"]:
                    # The observations we use
                    agentLoc = frame.agentLoc
                    ballLoc = frame.ballLoc
                    teammate_loc = frame.teammateLoc
                    opponent_loc = frame.opponentLoc
                    # check if all four properties are not None
                    assert agentLoc is not None
                    assert ballLoc is not None
                    # assert teammate_loc is not None
                    # assert opponent_loc is not None

                    obs = OBS.getObservation(agentLoc, ballLoc)
                    prevAgentLoc = agentLoc if prevAgentLoc is None else prevAgentLoc
                    prevBallLoc = ballLoc if prevBallLoc is None else prevBallLoc
                    reward = OBS.getReward(
                        agentLoc,
                        prevAgentLoc,
                        ballLoc,
                        prevBallLoc,
                        OBS.ballInGoal(ballLoc),
                        OBS.ballOutOfFieldBounds(ballLoc),
                        OBS.ballInGoalArea(ballLoc),
                        4,
                    )
                    act = None

                    if act is None:
                        act = [
                            *frame.motionBasics,
                            *frame.kickBasics,
                            # -7,  # stand
                        ]

                    if act is None:
                        raise Exception("acts is None")
                    frame.threadIndex
                    prev_state = state

                    info = {
                        "frameIndex": frame.absIndex,
                        "timestamp": frame.timestamp,
                        "agentLoc": agentLoc,
                        "ballLoc": ballLoc,
                        "robotSpeedX": act[0],
                        "robotSpeedY": act[1],
                        "robotSpeedRot": act[2],
                        "kickType": frame["MotionRequest"]["kickType"],
                        "kickLength": (
                            frame["MotionRequest"]["kickLength"]
                            if frame["MotionRequest"]["kickLength"] < 10e7
                            else -1
                        ),
                        "alignPreciselyModified": frame["MotionRequest"][
                            "alignPrecisely"
                        ],
                        "rollOutResult": frame.rollOutResult,
                        "jsonFile": frame.jsonName,
                    }
                    line = list(info.values())

                    if frame.absIndex > lastCsvFrame:
                        writer.writerow(line)
                    print(line)
                    # if prev_obs is not None:
                    #     transitions.append([prev_obs, acts, None, obs, False])
                    transitions.append([obs, act, None, reward, False])
                    prevObs = obs
                    OBS.stepObservationHistory(obs)

                    # obs, acts, infos, next_obs, dones
                elif state != GAME_STATE["playing"]:
                    if (
                        prev_state == GAME_STATE["playing"]
                        and state == GAME_STATE["ownKickOff"]
                    ):
                        if frame.absIndex > lastCsvFrame:
                            writer.writerow(
                                [
                                    frame.index,
                                    *(["-"] * 9),
                                    frame.rollOutResult,
                                    frame.jsonName,
                                ]
                            )
                        last = transitions[-1]
                        last[4] = True

                        obs, act, info, rewards, dones = map(
                            np.array, zip(*transitions)
                        )
                        np.savez(
                            LOG.outputDir / f"traj_{frame.absIndex}.npz",
                            obs=obs,
                            acts=act,
                            infos=info,
                            rewards=rewards,
                            dones=dones,
                        )
                        print(f"Saved {LOG.outputDir / f'traj_{frame.absIndex}.npz'}")
                else:
                    raise Exception(f"Unknown state: {state}")
                prev_state = state
            except (
                KeyError
            ) as e:  # This is normal, some frame simply doesn't have enough information
                print(f"KeyError: {e} at frame {frame.absIndex}")
            except AssertionError as e:
                print(f"AssertionError: {e} at frame {frame.absIndex}")
            except Exception as e:  # Just to add some robusty
                print(f"Exception: {e} at frame {frame.absIndex}")
                raise


profileFunction(main)
# main()
# with open("profile_result.prof", "w") as f:
#     stats = pstats.Stats(cProfile.Profile(), stream=f)
#     stats.sort_stats(pstats.SortKey.TIME)
#     stats.print_stats()
print("Done")
