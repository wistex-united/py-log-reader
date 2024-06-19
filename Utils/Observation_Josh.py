"""
Observation of Josh Kelle's branch
=====================

Author: Yuhao Li
Date: 2024-06-17
Description: This script reconstruct observations of BadgerRL res-josh-random-scenes branch.
"""

import math


class Observation:
    def __init__(self, policy_type):
        self.policyVars = self.getPolicyVars(policy_type)
        self.history_length = self.policyVars["history_length"]
        self.history = []

    def getPolicyVars(self, policy_type):
        teamWorkVars = {"history_length": 3}

        walkToBallVars = {"history_length": 0}

        policyVars = {
            "TeamWork": teamWorkVars,
            "WalkToBall": walkToBallVars,
        }

        return policyVars[policy_type]

    def getRelativeObservation(self, agent_loc, object_loc):
        x = object_loc[0] - agent_loc[0]
        y = object_loc[1] - agent_loc[1]
        angle = math.atan2(y, x) - agent_loc[2]
        xprime = x * math.cos(-agent_loc[2]) - y * math.sin(-agent_loc[2])
        yprime = x * math.sin(-agent_loc[2]) + y * math.cos(-agent_loc[2])
        return [xprime / 10000, yprime / 10000, math.sin(angle), math.cos(angle)]

    def getObservation(self, agent_loc, ball_loc):
        observation = []

        # Get relative position of ball to agent
        relative_observation = self.getRelativeObservation(agent_loc, ball_loc)
        observation.extend(relative_observation)

        # Get relative position to right goal post
        relative_observation_2 = self.getRelativeObservation(agent_loc, [4500, -800])
        observation.extend(relative_observation_2)

        # Get relative position to left goal post
        relative_observation_3 = self.getRelativeObservation(agent_loc, [4500, 800])
        observation.extend(relative_observation_3)

        # Add history
        for i in range(self.history_length):
            if len(self.history) > i:
                observation.extend(self.history[i])
            else:
                observation.extend([0] * 8)

        return observation

    def stepObservationHistory(self, observation):
        # Add the new observation at the end
        self.history.append(observation)

        # If the size of history exceeds history_length, remove the oldest observation
        if len(self.history) > self.history_length:
            self.history.pop(0)

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

    def getReward(
        self,
        cur_agent_loc,
        prev_agent_loc,
        cur_ball_loc,
        prev_ball_loc,
        is_goal,
        ball_is_out_of_field_bounds,
        ball_is_in_goal_area,
        frames_since_last_reward,
    ):
        # weights
        weight_dist_to_ball = 0.5 / max(
            1, frames_since_last_reward
        )  # ranges from about -2 to +2
        weight_ball_to_goal = 10 / max(1, frames_since_last_reward)
        weight_body_pointed_to_ball = 2
        weight_goal = 1000
        weight_ball_oob = -100
        weight_ball_outside_goal_area = -100
        weight_time = -0.1
        weight_lined_up = 1

        reward = 0

        # reward getting closer to the ball
        prev_dist = self.dist(prev_agent_loc, prev_ball_loc)
        cur_dist = self.dist(cur_agent_loc, cur_ball_loc)
        reward += weight_dist_to_ball * (prev_dist - cur_dist)

        # reward facing the ball (robot's body, not head)
        angle_to_ball = self.getAngleBetweenAgentAndBall(cur_agent_loc, cur_ball_loc)
        tolerance = 10
        max_angle_allowed = 40
        reward += weight_body_pointed_to_ball * self.clip(
            0, 1, 1 - (angle_to_ball - tolerance) / (max_angle_allowed - tolerance)
        )

        if cur_agent_loc[0] > cur_ball_loc[0]:
            reward += -5

        # reward lining up with the goal; No reward if the agent is already lined up within 15 degrees
        prev_lineup_gap = self.getLineupAngleDifference(prev_agent_loc, prev_ball_loc)
        cur_lineup_gap = self.getLineupAngleDifference(cur_agent_loc, cur_ball_loc)
        if prev_lineup_gap > 15 or cur_lineup_gap > 15:
            gap_reduction = prev_lineup_gap - cur_lineup_gap
            reward += weight_lined_up * gap_reduction

        # reward moving the ball toward the goal
        goal_post_left = [4500, 800]
        goal_post_right = [4500, -800]
        prev_dist_avg = (
            self.dist(prev_ball_loc, goal_post_left)
            + self.dist(prev_ball_loc, goal_post_right)
        ) / 2
        cur_dist_avg = (
            self.dist(cur_ball_loc, goal_post_left)
            + self.dist(cur_ball_loc, goal_post_right)
        ) / 2
        reward += weight_ball_to_goal * (prev_dist_avg - cur_dist_avg)

        if is_goal:
            reward += weight_goal
        elif ball_is_out_of_field_bounds:
            reward += weight_ball_oob
        elif not ball_is_in_goal_area:
            reward += weight_ball_outside_goal_area

        reward += weight_time

        return reward

    def dist(self, loc1, loc2):
        return math.sqrt((loc1[0] - loc2[0]) ** 2 + (loc1[1] - loc2[1]) ** 2)

    def normAngle180(self, angle):
        return math.fmod(angle + 180.0, 360.0) - 180.0

    def getAngleBetweenAgentAndBall(self, agent_loc, ball_loc):
        angle = self.angleBetween(agent_loc, ball_loc)
        return abs(self.normAngle180(angle))

    def angleBetween(self, agent_loc, ball_loc):
        # agent_loc is x, y, angle
        # ball_loc is x, y
        x = ball_loc[0] - agent_loc[0]
        y = ball_loc[1] - agent_loc[1]
        angle_between_robot_body_and_ball = math.atan2(y, x) - agent_loc[2]
        angle_between_robot_body_and_ball *= 180.0 / math.pi
        angle_between_robot_body_and_ball = math.fmod(
            angle_between_robot_body_and_ball, 360.0
        )
        return angle_between_robot_body_and_ball

    def getLineupAngleDifference(self, agent_loc, ball_loc):
        angleAgentBall = self.angleBetween(agent_loc, ball_loc)
        angleAgentGoal = self.angleBetween(agent_loc, [4500, 0])
        return abs(self.normAngle180(angleAgentBall - angleAgentGoal))

    def clip(self, min_value, max_value, value):
        return max(min_value, min(max_value, value))

    def ballInGoal(self, fieldBall):
        abs_x = abs(fieldBall[0])
        abs_y = abs(fieldBall[1])
        return (4500 < abs_x < 5055) and (abs_y < 800)

    def ballInGoalArea(self, fieldBall):
        abs_x = abs(fieldBall[0])
        abs_y = abs(fieldBall[1])
        return (3900 < abs_x < 4500) and (abs_y < 1100)

    def ballOutOfFieldBounds(self, fieldBall):
        abs_x = abs(fieldBall[0])
        abs_y = abs(fieldBall[1])
        return (abs_y > 3000 or abs_x > 4500) and not self.ballInGoal(fieldBall)


if __name__ == "__main__":
    obs = Observation("WalkToBall")
    # print(obs.getObservation([0, 0, 0], [100, 100]))
    print(
        obs.getReward(
            [4090.11132812, -1792.11596680, 1.48717415],
            [4083.33984375, -1801.69055176, 1.51695311],
            [4296.33398438, 464.37792969],
            [4222.21728516, 459.94738770],
            False,
            False,
            True,
            4,
        )
    )
