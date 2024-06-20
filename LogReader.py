import csv
from pathlib import Path
from typing import List

from LogInterface import Log
from Primitive import *
from StreamUtils import *
from Utils import ObservationJosh


def main():
    LOG = Log()
    # LOG.readLogFile("bc18_adam.log")
    # LOG.readLogFile("traj9_bh.log")
    LOG.readLogFile("traj12_formal.log")
    LOG.eval()
    LOG.parseBytes()
    # for frame in tqdm.tqdm(LOG.frames):
    #     frame.saveFrameDict()
    #     frame.saveImageWithMetaData(slientFail=True)
    # return
    

    OBS = ObservationJosh("WalkToBall")
    index = 0

    collisions = []
    prev_state = None
    prev_obs = None
    transitions: List[List] = []
    with open(f"{Path(LOG.logFilePath).stem}/data.csv", "w") as f:
        writer = csv.writer(f)
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
        prevBallLoc=None
        for frame in LOG.UncompressedChunk.threads["Cognition"]:
            # if frame.hasImage:
            #     frame.saveImageWithMetaData()
            #     print(f"Frame {frame.index} has image")
            # frame.recoverTrajectory()
            GAME_STATE = LOG.TypeInfoChunk.enumClasses["GameState::State"]
            try:
                gameState = frame["GameState"]
                playerNumber = gameState["playerNumber"]

                state = gameState["state"]

                if state == GAME_STATE["playing"]:
                    # The observations we use
                    agentLoc = [
                        frame["RobotPose"]["translation"].x,
                        frame["RobotPose"]["translation"].y,
                        frame["RobotPose"]["rotation"].value,
                    ]
                    ballLoc = [
                        frame["FieldBall"]["positionOnField"].x,
                        frame["FieldBall"]["positionOnField"].y,
                    ]
                    teammate_loc = []
                    for teammate in frame["GlobalTeammatesModel"]["teammates"]:
                        teammate_loc.append(
                            [teammate.pose.translation.x, teammate.pose.translation.y]
                        )

                    opponent_loc = []
                    for opponent in frame["GlobalOpponentsModel"]["opponents"]:
                        # Have to convert to global coordinates
                        opponent_loc.append(
                            [
                                opponent.position.x
                                + frame["RobotPose"]["translation"].x,
                                opponent.position.y
                                + frame["RobotPose"]["translation"].y,
                            ]
                        )
                    obs = OBS.getObservation(
                        agentLoc,
                        ballLoc
                    )
                    prevAgentLoc=agentLoc if prevAgentLoc is None else prevAgentLoc
                    prevBallLoc=ballLoc if prevBallLoc is None else prevBallLoc
                    reward=OBS.getReward(agentLoc,prevAgentLoc,ballLoc,prevBallLoc,OBS.ballInGoal(ballLoc),OBS.ballOutOfFieldBounds(ballLoc),OBS.ballInGoalArea(ballLoc),0)
                    # print(frame["MotionRequest"]["kickType"])
                    # print(frame["MotionRequest"]["kickLength"])
                    # print(frame["MotionRequest"]["alignPrecisely"])
                    # print(frame["MotionRequest"]["targetDirection"])
                    kickLength = frame["MotionRequest"]["kickLength"]

                    acts = None
                    # for message in frame.Annotations:
                    #     if message["name"] == "policy_action":
                    #         acts = ast.literal_eval(
                    #             "[" + message["annotation"][1:-1] + "]"
                    #         )
                    alignPreciselyModified = 0
                    if frame["MotionRequest"]["alignPrecisely"].value == 0:
                        alignPreciselyModified = 1
                    elif frame["MotionRequest"]["alignPrecisely"].value == 1:
                        alignPreciselyModified = 0
                    elif frame["MotionRequest"]["alignPrecisely"].value == 2:
                        alignPreciselyModified = 0.5

                    if acts is None:
                        acts = [
                            frame["MotionInfo"]["speed"]["translation"].x,
                            frame["MotionInfo"]["speed"]["translation"].y,
                            frame["MotionInfo"]["speed"]["rotation"],
                            frame["MotionRequest"]["kickType"],
                            kickLength if kickLength < 1e7 else 0,
                            alignPreciselyModified,
                            # -7,  # stand
                        ]

                    if acts is None:
                        raise Exception("acts is None")

                    writer.writerow(
                        [
                            frame.index,
                            agentLoc,
                            ballLoc,
                            *acts,
                            frame["GameControllerData"]["rollOutResult"],
                            frame.jsonName,
                        ]
                    )

                    # if prev_obs is not None:
                    #     transitions.append([prev_obs, acts, None, obs, False])
                    
                    prev_obs = obs
                    OBS.stepObservationHistory(obs) 

                    # obs, acts, infos, next_obs, dones
                elif state != GAME_STATE["playing"]:
                    if (
                        prev_state == GAME_STATE["playing"]
                        and state == GAME_STATE["ownKickOff"]
                    ):
                        writer.writerow(
                            [
                                frame.index,
                                *(["-"] * 8),
                                frame["GameControllerData"]["rollOutResult"],
                                frame.jsonName,
                            ]
                        )
                        transitions.pop()  # Remove the last transition that might be wrong
                        # Set the done flag for the last state
                        last = transitions[-1]
                        last[4] = True

                # elif state == GAME_STATE["ownKickOff"]:
                #     pass
                # elif state == GAME_STATE["setupOpponentKickOff"]:
                #     pass
                # elif state == GAME_STATE["waitForOpponentKickOff"]:
                #     pass
                # elif state == GAME_STATE["opponentKickOff"]:
                #     pass
                # elif state == GAME_STATE["opponentGoalKick"]:
                #     pass
                else:
                    print(gameState["state"])
                prev_state = state
            except KeyError as e:
                collisions.append((frame.index, frame.classNames, e))

            # if len(frame.dummyMessages)!=0:
            #     print(1)
        # print(collisions)

    obs, acts, infos, next_obs, dones = map(np.array, zip(*transitions))
    np.savez(
        f"{Path(LOG.logFilePath).stem}/transitions.npz",
        obs=obs,
        acts=acts,
        infos=infos,
        next_obs=next_obs,
        dones=dones,
    )

    #obs, acts, infos, rewards, terminate(bool) 


    # imitationLearningTransitions=Transitions(obs, acts, infos, next_obs, dones)


# profiler = cProfile.Profile()
# # Start profiling
# profiler.enable()

# # Code you want to profile
main()

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
