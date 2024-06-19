"""
Observation of adam's branch
=====================

Author: Yuhao Li
Date: 2024-05-24
Description: This script reconstruct observations of BadgerRL res-adam branch.
"""

import math

import numpy as np


class Observation:
    def __init__(self, policy_type):
        self.policyVars = self.getPolicyVars(policy_type)
        self.history_length = self.policyVars["history_length"]
        self.individual_observation_length = self.policyVars[
            "individual_observation_length"
        ]
        self.history = {}

    def getPolicyVars(self, policy_type):
        soccerVars = {
            "history_length": 3,
            "individual_observation_length": 25,
            "num_teammates": 1,
            "num_opponents": 0,
        }

        staticDef = {
            "history_length": 3,
            "individual_observation_length": 25,  # TODO: Get this
            "num_teammates": 1,
            "num_opponents": 2,
        }

        ballToPointVars = {
            "history_length": 3,
            "individual_observation_length": 25,
            "num_teammates": 1,
            "num_opponents": 0,
        }

        walkToBallVars = {
            "history_length": 3,
            "individual_observation_length": 12,
            "num_teammates": 0,
            "num_opponents": 0,
        }

        policyVars = {
            "soccer": soccerVars,
            "staticDef": staticDef,
            "ballToPoint": ballToPointVars,
            "WalkToBall": walkToBallVars,
        }

        return policyVars[policy_type]

    def stepObservationHistory(self, agent_loc, ball_loc, robotNum):
        relative_observation = self.get_relative_observation(agent_loc, ball_loc)

        for i in range(1, self.history_length):
            self.history[robotNum][i - 1] = self.history[robotNum][i]

        self.history[robotNum][self.history_length - 1] = relative_observation

    def get_relative_observation(self, agent_loc, object_loc):
        x = object_loc[0] - agent_loc[0]
        y = object_loc[1] - agent_loc[1]
        angle = math.atan2(y, x) - agent_loc[2]
        xprime = x * math.cos(-agent_loc[2]) - y * math.sin(-agent_loc[2])
        yprime = x * math.sin(-agent_loc[2]) + y * math.cos(-agent_loc[2])
        return [xprime / 10000, yprime / 10000, math.sin(angle), math.cos(angle)]

    def canKick(self, agent_loc, ball_loc):
        # Placeholder for the actual logic to determine if the agent can kick the ball.
        distance = math.sqrt(
            (ball_loc[0] - agent_loc[0]) ** 2 + (ball_loc[1] - agent_loc[1]) ** 2
        )
        # angle = math.degrees(
        #     math.atan2(ball_loc[1] - agent_loc[1], ball_loc[0] - agent_loc[0])
        # )
        return distance < 300 and self.checkFacingBall(agent_loc, ball_loc)

    def checkFacingBall(self, agent_loc, ball_loc, req_angle=18):
        """
        Check if the agent is facing the ball within a specified angle.

        :param agent_loc: List of the agent's location and orientation [x, y, theta]
        :param ball_loc: List of the ball's location [x, y]
        :return: True if the agent is facing the ball, False otherwise
        """
        # Define the required angle
        # req_angle = 18  # Adjust this value as needed

        # Convert from radians to degrees
        robot_angle = math.degrees(agent_loc[2]) % 360

        # Find the angle between the robot and the ball
        angle_to_ball = math.degrees(
            math.atan2(ball_loc[1] - agent_loc[1], ball_loc[0] - agent_loc[0])
        )

        # Check if the robot is facing the ball
        angle = (robot_angle - angle_to_ball + 360) % 360

        if angle < req_angle or angle > 360 - req_angle:
            return True
        else:
            return False

    def getObservation(
        self, agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum, policy_type
    ):
        if policy_type == "soccer":
            return self.getSoccerObservation(
                agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
            )
        elif policy_type == "staticDef":
            return self.getStaticDefObservation(
                agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
            )
        elif policy_type == "ballToPoint":
            return self.getBallToPointObservation(
                agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
            )
        else:
            return self.getSoccerObservation(
                agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
            )

    def getSoccerObservation(
        self, agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
    ):
        if robotNum not in self.history:
            self.history[robotNum] = [[0, 0, 0, 0] for _ in range(self.history_length)]

        observation = []
        relative_observation = self.get_relative_observation(agent_loc, ball_loc)
        observation.extend(relative_observation)
        observation.append(1 if self.canKick(agent_loc, ball_loc) else 0)

        for i in range(self.policyVars["num_teammates"]):
            if i < len(teammate_loc):
                relative_observation = self.get_relative_observation(
                    agent_loc, teammate_loc[i]
                )
            else:
                relative_observation = self.get_relative_observation(
                    agent_loc, [-4800, 3500]
                )
            observation.extend(relative_observation)

        goal_posts = [[4500, 250], [4500, -250], [-4500, 250], [-4500, -250]]
        for post in goal_posts:
            relative_observation = self.get_relative_observation(agent_loc, post)
            observation.extend(relative_observation)

        sides = [[0, 3000], [0, -3000]]
        for side in sides:
            relative_observation = self.get_relative_observation(agent_loc, side)
            observation.extend(relative_observation)

        for history_entry in self.history[robotNum]:
            observation.extend(history_entry)

        return np.array(observation)

    def getStaticDefObservation(
        self, agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
    ):
        raise NotImplementedError("getStaticDefObservation not implemented")
        return []

    def getBallToPointObservation(
        self, agent_loc, ball_loc, teammate_loc, opponent_loc, robotNum
    ):
        raise NotImplementedError("getBallToPointObservation not implemented")
        return []


if __name__ == "__main__":
    obs = Observation("soccer")
    print(
        obs.getObservation(
            [0, 0, 0], [100, 100], [[200, 200], [300, 300]], [[400, 400]], 1, "soccer"
        )
    )
