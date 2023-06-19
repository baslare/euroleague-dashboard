from dataclasses import dataclass

import pandas as pd
from dataclasses_json import dataclass_json
from processing.processing_functions import make_pbp_df, make_players_df, make_points_df


@dataclass_json
@dataclass
class GameData:
    season: int
    game_code: int
    home_team: str
    away_team: str
    points: dict
    home_players: list
    away_players: list
    play_by_play: dict

    def __post_init__(self):

        self.play_by_play = make_pbp_df(self.play_by_play)
        self.play_by_play.loc[[0, self.play_by_play.shape[0] - 1], "CODETEAM"] = f"{self.home_team}-{self.away_team}"

        self.home_players = make_players_df(self.home_players)
        self.away_players = make_players_df(self.away_players)

        self.points = make_points_df(self.points)



    def get_opponent_pbp(self, home=True):
        team = self.home_team if home else self.away_team
        return self.play_by_play.loc[self.play_by_play["CODETEAM"].str.match(team), :]





