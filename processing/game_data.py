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

        self.lineups_home: pd.DataFrame
        self.lineups_away: pd.DataFrame

    def get_pbp(self, home=True):
        team = self.home_team if home else self.away_team
        return self.play_by_play.loc[self.play_by_play["CODETEAM"].str.contains(team), :].reset_index().rename(
            columns={"index": "index_pbp"})

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
        subs_df["index_right"] = subs_df["index_left"].tolist()[1:] + [pbp.shape[0] - 1]

        pbp_lineups = [None] * pbp.shape[0]
        left_right_indices = [None] * pbp.shape[0]
        pbp_lineups[0:subs_df["index_left"][0]] = [self.get_starting_lineup()] * subs_df["index_left"][0]
        left_right_indices[0:subs_df["index_left"][0]] = [[0, subs_df["index_left"][0]]] * subs_df["index_left"][0]

        for x, y, z in zip(subs_df["index_left"], subs_df["index_right"] + 1, subs_df["lineups"]):
            pbp_lineups[x:y] = [z] * (y - x)
            left_right_indices[x:y] = [[x, y]] * (y - x)

        pbp["lineups"] = pbp_lineups
        pbp["index_left"] = pd.Series([x[0] for x in left_right_indices])
        pbp["index_right"] = pd.Series([x[1] for x in left_right_indices])
        # pbp = pbp.loc[~pbp["playerIn"] & ~pbp["playerOut"], :]
        # pbp = pbp.drop(columns=["playerIn", "playerOut"])

        return pbp.reset_index(drop=True)

    def extract_home_away_lineups(self):
        self.lineups_home = self.get_lineups()
        self.lineups_away = self.get_lineups(home=False)

    def stat_calculator(self, home=True):

        df = self.lineups_home if home else self.lineups_away
        replace_dict = {"2FGAB": "2FGA", "LAYUPATT": "2FGA", "LAYUPMD": "2FGM", "DUNK": "2FGM"}
        df.loc[:, "PLAYTYPE"] = df["PLAYTYPE"].map(lambda rep: replace_dict.get(rep, rep))

        time_comp = pd.Series([0] + df["time"].tolist()[:-1])
        df["duration"] = df["time"] - time_comp

        stat_keys = ["AS", "TO", "3FGM", "2FGM", "FTM", "D", "O", "RV", "CM", "FV", "AG", "ST", "OF", "CMT"]

        for x in stat_keys:
            df[x] = df["PLAYTYPE"] == x

        stat_composite_keys = {"3FGA": "3FGA|3FGM",
                               "2FGA": "2FGA|2FGM",
                               "FTA": "FTA|FTM",
                               "REB": "D$|O$"}

        for key, val in zip(stat_composite_keys, stat_composite_keys.values()):
            df[key] = df["PLAYTYPE"].str.match(val)

    def opp_stat_calculator(self, home=True):

        df_lineups = self.lineups_home if home else self.lineups_away
        df_lineups_opp = self.lineups_away if home else self.lineups_home

        stat_keys = ["duration", "AS", "TO", "3FGM", "3FGA", "2FGM",
                     "3FGA", "FTM", "FTA", "D", "O", "REB",
                     "RV", "CM", "FV", "AG", "ST", "OF", "CMT"]

        stat_dict = {x: "sum" for x in stat_keys}

        df_lineups["lineups_string"] = df_lineups["lineups"].apply(lambda x: "; ".join(x))

        df = df_lineups.groupby(["lineups_string", "CODETEAM", "index_left", "index_right"]).agg(
            stat_dict).reset_index()

        df = df.loc[df["duration"] != 0,:]
        df["min"] = df.apply(lambda x: df_lineups["time"].iloc[x["index_left"]], axis=1)
        df["max"] = df.apply(lambda x: df_lineups["time"].iloc[x["index_right"] - 1], axis=1)
        df = df.loc[df["min"] != df["max"], :]

        game_epochs = sorted(pd.concat([df["min"], df["max"]]).unique().tolist())

        df_lineups["game_epochs"] = pd.cut(df_lineups["time"], game_epochs, include_lowest=True, right=False).astype(str)
        df_lineups_opp["game_epochs"] = pd.cut(df_lineups_opp["time"], game_epochs, include_lowest=True, right=False).astype(str)

        df = df_lineups.groupby(["game_epochs", "lineups_string"]).agg(
            stat_dict).reset_index()

        df_opp = df_lineups_opp.groupby("game_epochs").agg(stat_dict).reset_index()

        df_opp.to_csv("opp.csv")

        return df

    def possession_finder(self):
        pass
