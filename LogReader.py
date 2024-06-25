import csv
import os
from pathlib import Path
from typing import List
import linecache

import tqdm, psutil, tracemalloc
import tracemalloc
from pympler import tracker, summary, muppy
from LogInterface import Log
from Primitive import *
from StreamUtils import *
from Utils import ObservationJosh
from Utils import countLines, readLastLine, extractTrajNumbers


def start_tracing():
    tracemalloc.start()


def display_top(snapshot, key_type="lineno", limit=10):
    snapshot = snapshot.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    top_stats = snapshot.statistics(key_type)

    print(f"Top {limit} lines")
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        print(f"#{index}: {frame.filename}:{frame.lineno} - {stat.size / 1024:.1f} KiB")
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print(f"    {line}")

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print(f"{len(other)} other: {size / 1024:.1f} KiB")
    total = sum(stat.size for stat in top_stats)
    print(f"Total allocated size: {total / 1024:.1f} KiB")


def stop_tracing():
    snapshot = tracemalloc.take_snapshot()
    display_top(snapshot)


def report_memory_usage():
    all_objects = muppy.get_objects()
    sum_objects = summary.summarize(all_objects)
    summary.print_(sum_objects)

    tr = tracker.SummaryTracker()
    tr.print_diff()


def checkPointCallback(cnt):
    if cnt % 1000 == 0:
        print(f"Checkpoint: {cnt}")
        snapshot = tracemalloc.take_snapshot()
        display_top(snapshot)


def main():
    start_tracing()
    LOG = Log()
    # LOG.readLogFile("bc18_adam.log")
    # LOG.readLogFile("traj9_bh.log")
    LOG.readLogFile("traj16_formal.log")
    LOG.eval()
    # LOG.parseBytes()
    # for frame in tqdm.tqdm(LOG.frames):
    #     frame.saveFrameDict()
    #     frame.saveImageWithMetaData(slientFail=True)
    # return

    OBS = ObservationJosh("WalkToBall")
    index = 0

    collisions = []
    prev_state = None
    prevObs = None
    transitions: List[List] = []

    lastCsvFrame = 0
    lastTrajFrame = 0
    if LOG.outputDir.exists():
        csvLine = countLines(LOG.outputDir / "data.csv")

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
    with open(LOG.outputDir / "data.csv", "a") as f:
        writer = csv.writer(f)
        if csvLine == 0:
            writer.writerow(
                [
                    "frameIndex",
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
                        0,
                    )
                    acts = None

                    if acts is None:
                        acts = [
                            *frame.motionBasics,
                            *frame.kickBasics,
                            # -7,  # stand
                        ]

                    if acts is None:
                        raise Exception("acts is None")
                    frame.threadIndex
                    prev_state = state

                    infos = {
                        "frameIndex": frame.absIndex,
                        "agentLoc": agentLoc,
                        "ballLoc": ballLoc,
                        "robotSpeedX": acts[0],
                        "robotSpeedY": acts[1],
                        "robotSpeedRot": acts[2],
                        "kickType": acts[3],
                        "kickLength": acts[4],
                        "alignPreciselyModified": acts[5],
                        "rollOutResult": frame.rollOutResult,
                        "jsonFile": frame.jsonName,
                    }
                    line = list(infos.values())
                    if frame.absIndex > lastCsvFrame:
                        writer.writerow(line)
                    print(line)
                    # if prev_obs is not None:
                    #     transitions.append([prev_obs, acts, None, obs, False])
                    transitions.append([obs, acts, infos, reward, False])
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
                                    *(["-"] * 8),
                                    frame.rollOutResult,
                                    frame.jsonName,
                                ]
                            )
                        last = transitions[-1]
                        last[4] = True

                        obs, acts, infos, next_obs, dones = map(
                            np.array, zip(*transitions)
                        )
                        np.savez(
                            LOG.outputDir / f"traj_{frame.absIndex}.npz",
                            obs=obs,
                            acts=acts,
                            infos=infos,
                            next_obs=next_obs,
                            dones=dones,
                        )
                        print(f"Saved {LOG.outputDir / f'traj_{frame.absIndex}.npz'}")
                else:
                    raise Exception(f"Unknown state: {state}")
                prev_state = state
            except KeyError as e:
                collisions.append((frame.absIndex, frame.classNames, e))
                print(
                    f"KeyError: {e} at frame {frame.absIndex} with {frame.classNames}"
                )
            except AssertionError as e:
                collisions.append((frame.absIndex, frame.classNames, e))
                print(
                    f"AssertionError: {e} at frame {frame.absIndex} with {frame.classNames}"
                )
            except Exception as e:
                print(f"Exception: {e} at frame {frame.absIndex}")
                raise

            # if len(frame.dummyMessages)!=0:
            #     print(1)
        # print(collisions)

    # obs, acts, infos, next_obs, dones = map(np.array, zip(*transitions))
    # np.savez(
    #     f"{Path(LOG.logFilePath).stem}/transitions.npz",
    #     obs=obs,
    #     acts=acts,
    #     infos=infos,
    #     next_obs=next_obs,
    #     dones=dones,
    # )

    # obs, acts, infos, rewards, terminate(bool)

    # imitationLearningTransitions=Transitions(obs, acts, infos, next_obs, dones)


# profiler = cProfile.Profile()
# # Start profiling
# profiler.enable()

# # Code you want to profile
main()
print("Done")
# # Stop profiling
# profiler.disable()
# # Create a Stats object
# stats = pstats.Stats(profiler).sort_stats("cumulative")
# # Print the stats
# # stats.print_stats()

# # Optionally, you can save the stats to a file
# stats.dump_stats("profile_stats.prof")
# Load the stats
# loaded_stats = pstats.Stats("profile_stats.prof")

# Print the stats
# loaded_stats.strip_dirs().sort_stats("cumulative").print_stats()
