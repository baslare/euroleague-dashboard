from dataclasses import dataclass
from dataclasses_json import dataclass_json
from processing.processing_functions import make_pbp_df, make_players_df, make_points_df
import re
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

        self.pbp_processed_home: pd.DataFrame
        self.pbp_processed_away: pd.DataFrame

        self.lineups_home: pd.DataFrame
        self.lineups_away: pd.DataFrame

    def get_pbp(self, home=True):
        team = self.home_team if home else self.away_team
        self.play_by_play = self.play_by_play.groupby("time").apply(lambda x: x.sort_values("PLAYTYPE")).reset_index(
            drop=True)

        return self.play_by_play.loc[self.play_by_play["CODETEAM"].str.contains(team), :].reset_index().rename(
            columns={"index": "index_pbp"})

    def get_starting_lineup(self, home=True):
        players = self.home_players if home else self.away_players
        players = players.loc[players["st"] == 1, :]
        return sorted(list(players["ac"]))

    def get_lineups(self, home=True):
        pbp = self.get_pbp(home)

        ins_and_outs = pbp.loc[pbp["playerIn"] | pbp["playerOut"], :]

        inc_player = ins_and_outs.loc[ins_and_outs["playerIn"], ["PLAYER_ID", "time"]].reset_index().rename(
            columns={"index": "index_inc",
                     "PLAYER_ID": "PLAYER_ID_IN"})
        out_player = ins_and_outs.loc[ins_and_outs["playerOut"], ["PLAYER_ID", "time"]].reset_index().rename(
            columns={"index": "index_out",
                     "PLAYER_ID": "PLAYER_ID_OUT"})

        inc_player = inc_player.groupby('time').agg(
            {"PLAYER_ID_IN": lambda x: list(x), "index_inc": lambda x: list(x)}).reset_index()
        out_player = out_player.groupby('time').agg(
            {"PLAYER_ID_OUT": lambda x: list(x), "index_out": lambda x: list(x)}).reset_index()

        subs_df = pd.concat([inc_player, out_player], axis=1)

        current_lineup = self.get_starting_lineup(home)

        def substitution(in_player, off_player):
            nonlocal current_lineup

            for off_player, in_player in zip(off_player, in_player):
                current_lineup.remove(off_player)
                current_lineup.append(in_player)

            return tuple(current_lineup)

        # because tuples are immutable
        lineups = ()

        for idx, tup in enumerate(zip(subs_df["PLAYER_ID_IN"], subs_df["PLAYER_ID_OUT"])):
            lineups = lineups + (substitution(tup[0], tup[1]),)

        lineups = list(lineups)
        subs_df["lineups"] = pd.Series([sorted(list(x)) for x in lineups])
        subs_df["index_left"] = subs_df.apply(lambda x: max(max(x["index_inc"]), max(x["index_out"])), axis=1)
        subs_df = subs_df.drop(columns=["index_inc", "index_out"])
        subs_df["index_right"] = subs_df["index_left"].tolist()[1:] + [pbp.shape[0]]

        pbp_lineups = [None] * pbp.shape[0]
        left_right_indices = [None] * pbp.shape[0]
        pbp_lineups[0:subs_df["index_left"][0]] = [self.get_starting_lineup()] * subs_df["index_left"][0]
        left_right_indices[0:subs_df["index_left"][0]] = [[0, subs_df["index_left"][0]]] * subs_df["index_left"][0]

        for x, y, z in zip(subs_df["index_left"], subs_df["index_right"], subs_df["lineups"]):
            pbp_lineups[x:y] = [z] * (y - x)
            left_right_indices[x:y] = [[x, y]] * (y - x)

        pbp["lineups"] = pbp_lineups
        pbp["index_left"] = pd.Series([x[0] for x in left_right_indices])
        pbp["index_right"] = pd.Series([x[1] for x in left_right_indices])
        pbp["OPP"] = self.away_team if home else self.home_team

        return pbp

    def extract_home_away_lineups(self):
        self.pbp_processed_home = self.get_lineups()
        self.pbp_processed_away = self.get_lineups(home=False)

    def stat_calculator(self, home=True):

        df = self.pbp_processed_home if home else self.pbp_processed_away
        replace_dict = {"2FGAB": "2FGA", "LAYUPATT": "2FGA", "LAYUPMD": "2FGM", "DUNK": "2FGM"}
        df.loc[:, "PLAYTYPE"] = df["PLAYTYPE"].map(lambda rep: replace_dict.get(rep, rep))

        time_comp = pd.Series([0] + df["time"].tolist()[:-1])
        df["duration"] = df["time"] - time_comp

        stat_keys = ["AS", "TO", "3FGM", "2FGM", "FTM", "D", "O", "RV",
                     "CM", "FV", "AG", "ST", "OF", "CMT"]

        for x in stat_keys:
            df[x] = df["PLAYTYPE"] == x

        stat_composite_keys = {"3FGA": "3FGA|3FGM",
                               "2FGA": "2FGA|2FGM",
                               "FTA": "FTA|FTM",
                               "REB": "D$|O$"}

        for key, val in zip(stat_composite_keys, stat_composite_keys.values()):
            df[key] = df["PLAYTYPE"].str.match(val)

    def opp_stat_calculator(self, home=True):

        df_lineups = self.pbp_processed_home if home else self.pbp_processed_away
        df_lineups["CODETEAM"] = self.home_team if home else self.away_team
        df_lineups_opp = self.pbp_processed_away if home else self.pbp_processed_home

        stat_keys = ["duration", "AS", "TO", "3FGM", "3FGA", "2FGA", "2FGM",
                     "3FGA", "FTM", "FTA", "D", "O", "REB",
                     "RV", "CM", "FV", "AG", "ST", "OF", "CMT",
                     "multi_ft", "assisted_2fg", "assisted_3fg",
                     "assisted_ft", "and_one_2fg", "and_one_3fg", "pos"]

        stat_dict = {x: "sum" for x in stat_keys}

        df_lineups["lineups_string"] = df_lineups["lineups"].apply(lambda x: "; ".join(x))

        df = df_lineups.groupby(["lineups_string", "CODETEAM", "index_left", "index_right"]).agg(
            stat_dict).reset_index()

        df["min"] = df.apply(lambda x: df_lineups["time"][x["index_left"]], axis=1)
        df["max"] = df.apply(lambda x: df_lineups["time"][x["index_right"] - 1], axis=1)
        df = df.loc[df["min"] != df["max"], :]

        game_epochs = sorted(pd.concat([df["min"], df["max"]]).unique().tolist())

        df_lineups["game_epochs"] = pd.cut(df_lineups["time"], game_epochs).astype(
            str)
        df_lineups_opp["game_epochs"] = pd.cut(df_lineups_opp["time"], game_epochs).astype(str)

        df = df_lineups.groupby(["game_epochs", "lineups_string", "CODETEAM","OPP"]).agg(
            stat_dict).reset_index()

        df = df.loc[df["duration"] != 0, :]

        df_opp = df_lineups_opp.groupby("game_epochs").agg(stat_dict).reset_index()
        rename_dict = {x: f"opp_{x}" for x in stat_keys}
        df_opp = df_opp.rename(columns=rename_dict)

        df = df.merge(df_opp, how="left", on="game_epochs")

        if home:
            self.lineups_home = df
        else:
            self.lineups_away = df

    def extra_stats_finder(self, home=True):
        pbp = self.pbp_processed_home if home else self.pbp_processed_away
        pbp_sub = pbp.groupby("time").apply(
            lambda x: list(x["PLAYTYPE"])).reset_index().rename(columns={0: "PLAYTYPE"})

        def finder(row):
            if len(row) > 1:
                multi_ft = sum([bool(re.match("FT", a)) for a in row]) > 1
                assisted_2fg = all([item in row for item in ["2FGM", "AS"]])
                assisted_3fg = all([item in row for item in ["3FGM", "AS"]])
                assisted_ft = multi_ft and "AS" in row
                and_one_2fg = all([item in row for item in ["2FGM", "RV"]]) and any(
                    [bool(re.match("FT", a)) for a in row])
                and_one_3fg = all([item in row for item in ["3FGM", "RV"]]) and any(
                    [bool(re.match("FT", a)) for a in row])

                return {"multi_ft": multi_ft,
                        "assisted_2fg": assisted_2fg,
                        "assisted_3fg": assisted_3fg,
                        "assisted_ft": assisted_ft,
                        "and_one_2fg": and_one_2fg,
                        "and_one_3fg": and_one_3fg}
            else:
                return {"multi_ft": False,
                        "assisted_2fg": False,
                        "assisted_3fg": False,
                        "assisted_ft": False,
                        "and_one_2fg": False,
                        "and_one_3fg": False}

        pbp_sub["extra_stats"] = pbp_sub["PLAYTYPE"].apply(lambda x: finder(x))
        pbp_sub2 = pd.json_normalize(pbp_sub["extra_stats"])
        pbp_sub = pbp_sub.drop("extra_stats", axis=1)
        pbp_sub = pd.concat([pbp_sub, pbp_sub2], axis=1)

        pbp_sub["check"] = pbp_sub["PLAYTYPE"].iloc[1:].reset_index(drop=True)
        pbp_sub.loc[pbp_sub.shape[0] - 1, "check"] = ["EG"]
        as2_check = ["2FGM" in x and "AS" in y for x, y in zip(pbp_sub["PLAYTYPE"], pbp_sub["check"])]
        as3_check = ["3FGM" in x and "AS" in y for x, y in zip(pbp_sub["PLAYTYPE"], pbp_sub["check"])]

        pbp_sub["assisted_2fg"] = pbp_sub["assisted_2fg"] | as2_check
        pbp_sub["assisted_3fg"] = pbp_sub["assisted_3fg"] | as3_check

        pbp_sub = pbp_sub.drop(["PLAYTYPE", "check"], axis=1)
        mask = pbp["time"].duplicated()
        col_names = [str(x) for x in pbp_sub.columns]
        pbp = pbp.merge(pbp_sub, how="left", on="time")
        pbp.loc[mask, col_names[1:]] = False

        pbp["pos"] = pbp["multi_ft"].astype(int) + pbp["2FGA"].astype(int) + pbp["3FGA"].astype(int) + pbp["TO"].astype(
            int) - pbp["O"].astype(int)

        if home:
            self.pbp_processed_home = pbp
        else:
            self.pbp_processed_away = pbp

    def get_player_stats(self,home=True):

        pbp = self.pbp_processed_home if home else self.pbp_processed_away
        lineup = self.lineups_home if home else self.lineups_away

        stat_keys = ["AS", "TO", "3FGM", "3FGA", "2FGA", "2FGM",
                     "3FGA", "FTM", "FTA", "D", "O", "REB",
                     "RV", "CM", "FV", "AG", "ST", "OF", "CMT",
                     "multi_ft", "assisted_2fg", "assisted_3fg",
                     "assisted_ft", "and_one_2fg", "and_one_3fg", "pos"]

        stat_dict = {x: "sum" for x in stat_keys}

        df = pbp.groupby(["PLAYER_ID", "CODETEAM", "OPP"]).agg(stat_dict).reset_index()
        df = df.loc[df["PLAYER_ID"] != "", :]

        player_df_list = []
        for x in df["PLAYER_ID"]:
            player_df_list.append(lineup.loc[lineup["lineups_string"].str.contains(x), :])

        # TODO fix player ratios merge
        player_df_list = [x[stat_keys].sum() for x in player_df_list]
        player_df_list = pd.concat(player_df_list,axis=1).transpose()
        player_df_list["PLAYER_ID"] = df["PLAYER_ID"]

        pass

    def get_team_stats(self):
        pass

    def replace_player_ids(self):
        pass
