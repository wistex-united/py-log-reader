import ast
import cProfile
import pstats
import tqdm
from imitation.data.types import Transitions

from LogInterface import Log
from Primitive import *
from StreamUtils import *
from Utils.Observation import Observation


def main():
    LOG = Log()
    LOG.readLogFile("bc15_adam.log")
    LOG.eval()
    LOG.parseBytes()
    # for frame in tqdm.tqdm(LOG.frames):
    #     frame.saveFrameDict()
    #     frame.saveImageWithMetaData(slientFail=True)
    # return

    OBS = Observation("soccer")
    index = 0

    collisions = []
    prev_state = None
    prev_obs = None
    transitions = []
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
                ball_loc = [
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
                            opponent.position.x + frame["RobotPose"]["translation"].x,
                            opponent.position.y + frame["RobotPose"]["translation"].y,
                        ]
                    )
                obs = OBS.getObservation(
                    agentLoc,
                    ball_loc,
                    teammate_loc,
                    opponent_loc,
                    playerNumber,
                    "soccer",
                )
                acts = []
                for message in frame.Annotations:
                    if message["name"] == "policy_action":
                        acts =  ast.literal_eval('[' + message["annotation"][1:-1] + "]")

                if prev_obs is not None:
                    transitions.append(())
                prev_obs = obs
                OBS.stepObservationHistory(agentLoc, ball_loc, playerNumber)
                # obs, acts, infos, next_obs, dones
            elif state != GAME_STATE["playing"]:
                if state != prev_state:
                    print(state)
                    print(frame.jsonName)
                pass
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


profiler = cProfile.Profile()
# Start profiling
profiler.enable()

# Code you want to profile
main()

# Stop profiling
profiler.disable()
# Create a Stats object
stats = pstats.Stats(profiler).sort_stats("cumulative")
# Print the stats
# stats.print_stats()

# Optionally, you can save the stats to a file
stats.dump_stats("profile_stats.prof")
# Load the stats
# loaded_stats = pstats.Stats("profile_stats.prof")

# Print the stats
# loaded_stats.strip_dirs().sort_stats("cumulative").print_stats()
