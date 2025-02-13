import csv
import os
import tracemalloc
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
    readLastLine,
    startMemoryTracing,
    WindowedProfiler,
)

profiler = WindowedProfiler(window_size=60)

def checkPointCallback(cnt):
    if cnt % 1000 == 0:
        print(f"Checkpoint: {cnt}")
        snapshot = tracemalloc.take_snapshot()
        displayTopMemoryConsumers(snapshot)

# @profiler.profile_with_windows
def main():
    LOG = Log()
    LOG.readLogFile("sample.log")
    # LOG.eval()
    LOG.eval(isLogFileLarge=True)

    # Dump all the representations into json and jpg images
    for frame in tqdm.tqdm(LOG.UncompressedChunk.thread("Cognition")):
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

    # if not (LOG.outputDir / "data.csv").exists():
    #     os.makedirs(LOG.outputDir)
    #     with open(LOG.outputDir / "data.csv", "w") as f:
    #         print("Create csv")
    #         pass
    GAME_STATE = LOG.TypeInfoChunk.enumClasses["GameState::State"]
    MOTION_REQUEST = LOG.TypeInfoChunk.enumClasses["MotionRequest::Motion"]

    with open(LOG.outputDir / "cog_data.csv", "w") as f:
        writer = csv.writer(f)
        if csvLine == 0:
            writer.writerow(
                [
                    "frameIndex",
                    "timestamp",
                    "agentLocX",
                    "agentLocY",
                    "agentLocRot",
                    "motionRequestX",
                    "motionRequestY",
                    "motionRequestRot",
                    "jsonFile",
                ]
            )

        for frame in LOG.UncompressedChunk.threads["Cognition"]:
            print(f"Frame {frame.indexCursor}")

            try:
                gameState = frame["GameState"]
                state = gameState["state"]

                if state != GAME_STATE["playing"]:
                    continue

                motionRequest = frame["MotionRequest"]
                if motionRequest["motion"] != MOTION_REQUEST["walkAtRelativeSpeed"]:
                    continue

                agentLoc = frame.agentLoc
                assert agentLoc is not None
                info = {
                    "frameIndex": frame.absIndex,
                    "timestamp": frame.timestamp,
                    "agentLocX": agentLoc[0],
                    "agentLocY": agentLoc[1],
                    "agentLocRot": agentLoc[2],
                    "motionRequestX":motionRequest["walkSpeed"]["translation"].x,
                    "motionRequestY":motionRequest["walkSpeed"]["translation"].y,
                    "motionRequestRot":motionRequest["walkSpeed"]["rotation"],
                    "jsonFile": frame.jsonName,
                }
                writer.writerow(list(info.values()))
            except (
                KeyError
            ) as e:  # This is normal, some frame simply doesn't have enough information
                print(f"KeyError: {e} at frame {frame.absIndex}")
            except AssertionError as e:
                print(f"AssertionError: {e} at frame {frame.absIndex}")
                print(str(frame))
                exit(1)
            except Exception as e:  # Just to add some robusty
                print(f"Exception: {e} at frame {frame.absIndex}")
                raise

    # with open(LOG.outputDir / "motion_data.csv", "w") as f:
    #     writer = csv.writer(f)
    #     if csvLine == 0:
    #         writer.writerow(
    #             [
    #                 "frameIndex",
    #                 "timestamp",
    #                 "agentSpeedX",
    #                 "agentSpeedY",
    #                 "agentSpeedRot",
    #                 "jsonFile",
    #             ]
    #         )

    #     for frame in LOG.UncompressedChunk.threads["Motion"]:
    #         print(f"Frame {frame.indexCursor}")

    #         try:

    #             motionBasics = frame.motionBasics
    #             assert motionBasics is not None
    #             info = {
    #                 "frameIndex": frame.absIndex,
    #                 "timestamp": frame.timestamp,
    #                 "agentSpeedX": motionBasics[0],
    #                 "agentSpeedY": motionBasics[1],
    #                 "agentSpeedRot": motionBasics[2],
    #                 "jsonFile": frame.jsonName,
    #             }
    #             writer.writerow(list(info.values()))
    #         except (
    #             KeyError
    #         ) as e:  # This is normal, some frame simply doesn't have enough information
    #             print(f"KeyError: {e} at frame {frame.absIndex}")
    #         except AssertionError as e:
    #             print(f"AssertionError: {e} at frame {frame.absIndex}")
    #             print(str(frame))
    #             exit(1)
    #         except Exception as e:  # Just to add some robusty
    #             print(f"Exception: {e} at frame {frame.absIndex}")
    #             raise


if __name__ == "__main__":
    main()
    print("Done")
