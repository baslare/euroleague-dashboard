from dataclasses import dataclass
from dataclasses_json import dataclass_json
from processing.processing_functions import make_pbp_df, make_players_df, make_points_df
import pandas as pd

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


    def get_pbp(self, home=True):
        team = self.home_team if home else self.away_team
        return self.play_by_play.loc[self.play_by_play["CODETEAM"].str.match(team), :].reset_index(drop=True)

    def get_starting_lineup(self, home=True):
        players = self.home_players if home else self.away_players
        players = players.loc[players["st"] == 1, :]
        return sorted(list(players["ac"]))

    def get_lineups(self, home=True):
        pbp = self.get_pbp(home)
        ins_and_outs = pbp.loc[pbp["playerIn"] | pbp["playerOut"], :]

        inc_player = ins_and_outs.loc[ins_and_outs["playerIn"], "PLAYER_ID"].reset_index().rename(
            columns={"index": "index_inc",
                     "PLAYER_ID": "PLAYER_ID_IN"})
        out_player = ins_and_outs.loc[ins_and_outs["playerOut"], "PLAYER_ID"].reset_index().rename(
            columns={"index": "index_out",
                     "PLAYER_ID": "PLAYER_ID_OUT"})

        subs_df = pd.concat([inc_player, out_player], axis=1)

        current_lineup = self.get_starting_lineup(home)

        def substitution(in_player, off_player):
            nonlocal current_lineup
            current_lineup.remove(off_player)
            current_lineup.append(in_player)
            return tuple(current_lineup)

        # because tuples are immutable
        lineups = ()

        for idx, tup in enumerate(zip(subs_df["PLAYER_ID_IN"], subs_df["PLAYER_ID_OUT"])):
            lineups = lineups + (substitution(tup[0], tup[1]),)

        lineups = list(lineups)
        subs_df["lineups"] = pd.Series([sorted(list(x)) for x in lineups])
        subs_df["index_left"] = subs_df.apply(lambda x: min(x["index_inc"], x["index_out"]), axis=1)
        subs_df = subs_df.drop(columns=["index_inc", "index_out"])
        subs_df["index_right"] = subs_df["index_left"].tolist()[1:] + [pbp.shape[0]]

        pbp_lineups = [None]*pbp.shape[0]
        pbp_lineups[0:subs_df["index_left"][0]] = [self.get_starting_lineup()]*subs_df["index_left"][0]

        for x, y, z in zip(subs_df["index_left"], subs_df["index_right"]+1, subs_df["lineups"]):
            pbp_lineups[x:y] = [z]*(y-x)

        pbp["lineups"] = pbp_lineups[:-1]
        pbp = pbp.loc[~pbp["playerIn"] & ~pbp["playerOut"], :]
        pbp = pbp.drop(columns=["playerIn", "playerOut"])

        return pbp

