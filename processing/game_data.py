from dataclasses import dataclass
from dataclasses_json import dataclass_json
from processing.processing_functions import make_pbp_df, make_players_df, make_points_df
import re
import pandas as pd
import numpy as np


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

        self.home_players_processed: pd.DataFrame
        self.away_players_processed: pd.DataFrame

        self.assists_home = pd.DataFrame
        self.assists_away = pd.DataFrame

        self.team_stats: pd.DataFrame
        self.home_team_name, self.away_team_name = self.get_team_names()

    def get_pbp(self, home=True):
        team = self.home_team if home else self.away_team
        self.play_by_play = self.play_by_play.groupby("time").apply(lambda x: x.sort_values("PLAYTYPE")).reset_index(
            drop=True)

        def swap_rows(df, row1, row2):
            df.iloc[row1, :], df.iloc[row2, :] = df.iloc[row2, :].copy(), df.iloc[row1, :].copy()
            return df

        end_game_check = self.play_by_play.iloc[-1]["PLAYTYPE"] == "EG"
        if end_game_check is False:
            idx = self.play_by_play.index.values[self.play_by_play['PLAYTYPE'] == "EG"][0]
            self.play_by_play = swap_rows(self.play_by_play, -1, idx).reset_index(drop=True)
            pass

        check_team = self.play_by_play["CODETEAM"].str.contains(team).tolist()
        return self.play_by_play.loc[check_team, :].reset_index().rename(
            columns={"index": "index_pbp"})

    def get_points_data(self):

        df = self.points
        df["season"] = self.season
        df.loc[:, "TEAM"] = df["TEAM"].str.replace(" ", "")
        df.loc[:, "ID_PLAYER"] = df["ID_PLAYER"].str.replace(" ", "")
        df["OPP"] = df["TEAM"].apply(lambda x: self.away_team if x == self.home_team else self.home_team)
        df["missed"] = df["ID_ACTION"].isin(["2FGA", "3FGA"])
        df = df[["ID_PLAYER", "TEAM", "OPP", "season", "PLAYER", "ID_ACTION", "COORD_X", "COORD_Y", "ZONE",
                 "missed"]].copy()
        df.loc[:, "game_code"] = self.game_code
        df["x_new"] = df["COORD_X"] * 416 / 1500 + 218
        df["y_new"] = df["COORD_Y"] * 776 / 2800 + 56
        df["home"] = np.where(df["TEAM"] == self.home_team, True, False)
        self.points = df.loc[~df["ID_ACTION"].isin(["FTM", "FTA"]), :]

    def get_starting_lineup(self, home=True):
        players = self.home_players if home else self.away_players
        players = players.loc[(players["st"] == 1) & (players["sl"] == 1) & (players["nn"] == 1), :].drop_duplicates(
            subset=["ac"], keep=False)
        starting_lineup = sorted(list(players["ac"]))
        return starting_lineup

    def get_team_names(self):

        team_name_dict = {
            "ASV": "Asvel",
            "BAR": "Barcelona",
            "BAS": "Baskonia",
            "BER": "Alba Berlin",
            "IST": "Efes",
            "MAD": "Real Madrid",
            "MCO": "Monaco",
            "MIL": "Milano",
            "MUN": "Bayern Munich",
            "OLY": "Olympiakos",
            "PAM": "Valencia",
            "PAN": "Panathinaikos",
            "PAR": "Partizan",
            "RED": "Crvena Zvezda",
            "TEL": "Maccabi Tel Aviv",
            "ULK": "Fenerbahce",
            "VIR": "Virtus",
            "ZAL": "Zalgiris"

        }

        return team_name_dict[self.home_team], team_name_dict[self.away_team]

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

        subs_df = pd.concat([inc_player, out_player], axis=1).copy()

        def length_finder(x):
            if type(x) == list:
                return len(x)
            elif x is None:
                return 0
            elif x is np.nan:
                return 0
            else:
                return 1

        subs_df_check = [length_finder(x) == length_finder(y) for x, y in zip(
            subs_df["PLAYER_ID_OUT"], subs_df["PLAYER_ID_IN"])]

        if all(subs_df_check) is False:
            subs_df_test = subs_df.loc[~pd.Series(subs_df_check), :]
            in_row = subs_df_test["PLAYER_ID_IN"].explode().dropna().tolist()
            out_row = subs_df_test["PLAYER_ID_OUT"].explode().dropna().tolist()

            in_row_idx = subs_df_test["index_inc"].explode().dropna().tolist()
            out_row_idx = subs_df_test["index_out"].explode().dropna().tolist()

            idx = min(subs_df_test.loc[:, "PLAYER_ID_IN"].index)

            subs_df.loc[:, "PLAYER_ID_IN"][idx] = in_row
            subs_df.loc[:, "PLAYER_ID_OUT"][idx] = out_row
            subs_df.loc[:, "index_inc"][idx] = in_row_idx
            subs_df.loc[:, "index_out"][idx] = out_row_idx

            subs_df = subs_df.dropna()

            pass

        current_lineup = self.get_starting_lineup(home)
        anti_zone = []
        prev_lineup = current_lineup

        def substitution(in_player, off_player):
            nonlocal current_lineup
            nonlocal anti_zone
            nonlocal prev_lineup

            err_count = 0
            prev_lineup = current_lineup
            current_lineup = [*current_lineup, *in_player]

            for x in off_player:
                try:
                    current_lineup.remove(x)
                    if len(anti_zone) > 0:
                        current_lineup.remove(anti_zone.pop())
                        prev_lineup = current_lineup
                except ValueError:
                    anti_zone.append(x)
                    err_count += 1

            if err_count > 0:
                return tuple(prev_lineup)
            else:
                return tuple(current_lineup)

        # because tuples are immutable
        lineups = ()

        for idx, tup in enumerate(zip(subs_df["PLAYER_ID_IN"], subs_df["PLAYER_ID_OUT"])):
            try:
                lineups = lineups + (substitution(tup[0], tup[1]),)
            except ValueError as err:
                pbp.to_csv("pbp.csv")

        lineups = list(lineups)
        subs_df["lineups"] = pd.Series([sorted(list(x)) for x in lineups])
        subs_df["index_left"] = subs_df.apply(lambda x: max(max(x["index_inc"]), max(x["index_out"])), axis=1)
        subs_df = subs_df.drop(columns=["index_inc", "index_out"])
        subs_df["index_right"] = subs_df["index_left"].tolist()[1:] + [pbp.shape[0]]

        pbp_lineups = [None] * pbp.shape[0]
        left_right_indices = [None] * pbp.shape[0]
        pbp_lineups[0:subs_df["index_left"][0]] = [self.get_starting_lineup()] * subs_df["index_left"][0] if home else [
                                                                                                                           self.get_starting_lineup(
                                                                                                                               home=False)] * \
                                                                                                                       subs_df[
                                                                                                                           "index_left"][
                                                                                                                           0]
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
                     "CM", "FV", "AG", "ST", "OF", "CMT", "CMU", "CMD"]

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
                     "RV", "CM", "FV", "AG", "ST", "OF", "CMT", "CMU", "CMD",
                     "multi_ft", "multi_ft_count", "tech_ft", "assisted_2fg", "assisted_3fg",
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

        df = df_lineups.groupby(["game_epochs", "lineups_string", "CODETEAM", "OPP"]).agg(
            stat_dict).reset_index()

        df = df.loc[df["duration"] != 0, :]

        df_opp = df_lineups_opp.groupby("game_epochs").agg(stat_dict).reset_index()
        rename_dict = {x: f"opp_{x}" for x in stat_keys}
        df_opp = df_opp.rename(columns=rename_dict)

        df = df.merge(df_opp, how="left", on="game_epochs")
        df["game_code"] = self.game_code

        if home:
            self.lineups_home = df
        else:
            self.lineups_away = df

    def extra_stats_finder(self, home=True):
        pbp = self.pbp_processed_home if home else self.pbp_processed_away
        pbp_sub = pbp.groupby(["time", "PLAYER_ID"]).apply(
            lambda x: list(x["PLAYTYPE"])).reset_index().rename(columns={0: "PLAYTYPE"})

        def finder(row):
            if len(row) > 1:
                multi_ft = sum([bool(re.match("FT", a)) for a in row]) > 1 or (
                        (sum([bool(re.match("FT", a)) for a in row]) == 1) &
                        any([bool(re.match("RV", a)) for a in row]) &
                        all([not (item in row) for item in ["3FGM", "2FGM"]])
                )

                and_one_2fg = all([item in row for item in ["2FGM", "RV"]]) and (sum(
                    [bool(re.match("FT", a)) for a in row]) == 1)
                and_one_3fg = all([item in row for item in ["3FGM", "RV"]]) and (sum(
                    [bool(re.match("FT", a)) for a in row]) == 1)
                tech_ft = (sum([bool(re.match("FT", a)) for a in row]) == 1) & (
                    all([not (item in row) for item in ["3FGM", "2FGM", "RV"]]))

                return {"multi_ft": multi_ft,
                        "and_one_2fg": and_one_2fg,
                        "and_one_3fg": and_one_3fg,
                        "tech_ft": tech_ft
                        }

            elif (len(row) == 1) & ((row[0] == "FTM") | (row[0] == "FTA")):

                tech_dict = {"multi_ft": False,
                             "and_one_2fg": False,
                             "and_one_3fg": False,
                             "tech_ft": True}

                return tech_dict

            else:
                return {"multi_ft": False,
                        "and_one_2fg": False,
                        "and_one_3fg": False,
                        "tech_ft": False}

        pbp_sub["extra_stats"] = pbp_sub["PLAYTYPE"].apply(lambda x: finder(x))
        pbp_sub2 = pd.json_normalize(pbp_sub["extra_stats"])
        pbp_sub3 = pbp.loc[pbp["PLAYTYPE"].isin(["2FGM", "3FGM", "AS", "FTM"]), ["PLAYER_ID", "PLAYTYPE", "time"]]
        pbp_sub3["PLAYTYPE"] = pbp_sub3["PLAYTYPE"].replace({"2FGM": "Two",
                                                             "3FGM": "Three",
                                                             "FTM": "FTM",
                                                             "AS": "AS"})

        pbp_sub3 = pbp_sub3.reset_index()
        pbp_sub3 = pbp_sub3.groupby("time").apply(lambda x: x.sort_values("PLAYTYPE", ascending=False)).reset_index(
            drop=True)

        assist_queue = []
        idx = pbp_sub3.shape[0] - 1
        pbp_sub3["assisting_player"] = np.nan

        # due to inconsistencies in the pbp, need to make sure assists are not being assigned to the same player
        # , but it is almost impossible to get every one of them right, a very small error margin to be expected
        while idx >= 0:

            if pbp_sub3.loc[:, "PLAYTYPE"].iloc[idx] == "AS":
                assist_queue.append(pbp_sub3.loc[:, "PLAYER_ID"].iloc[idx])
            elif len(assist_queue) > 0:
                if pbp_sub3.loc[idx, "PLAYER_ID"] == assist_queue[0]:
                    pbp_sub3.loc[idx, "assisting_player"] = assist_queue.pop()
                else:
                    pbp_sub3.loc[idx, "assisting_player"] = assist_queue.pop(0)
            else:
                pass

            idx -= 1

        pbp_sub3 = pbp_sub3.set_index("index")
        sub3_index = pbp_sub3.index
        pbp_sub3["index_to_fix"] = sub3_index

        pbp_sub3 = pbp_sub3.groupby("time").apply(
            lambda x: x.sort_values("assisting_player", ascending=False)).reset_index(
            drop=True)

        pbp_sub3 = pbp_sub3.set_index("index_to_fix")
        pbp_sub3["assisted_2fg"] = (pbp_sub3["PLAYTYPE"] == "Two") & (~pbp_sub3["assisting_player"].isna())
        pbp_sub3["assisted_3fg"] = (pbp_sub3["PLAYTYPE"] == "Three") & (~pbp_sub3["assisting_player"].isna())

        pbp_sub3["assisted_ft"] = (pbp_sub3["PLAYTYPE"] == "FTM") & (~pbp_sub3["assisting_player"].isna())

        pbp_sub = pbp_sub.drop("extra_stats", axis=1)
        pbp_sub = pd.concat([pbp_sub, pbp_sub2], axis=1)
        pbp_sub.loc[pbp_sub.shape[0] - 1, "check"] = ["EG"]
        pbp_sub = pbp_sub.drop(["PLAYTYPE", "check"], axis=1)
        mask = pbp.duplicated(["PLAYER_ID", "time"])

        col_names = [str(x) for x in pbp_sub.columns]
        pbp = pbp.merge(pbp_sub, how="left", on=["time", "PLAYER_ID"])
        pbp["multi_ft"] = pbp["multi_ft"] & pbp["FTA"]
        pbp["multi_ft_count"] = pbp["multi_ft"] & pbp["FTA"]
        pbp.loc[mask, col_names[3:]] = False

        ft_mask = pbp[["multi_ft_count", "time"]].duplicated()
        pbp.loc[ft_mask, "multi_ft"] = False

        pbp["assisted_2fg"] = pbp_sub3["assisted_2fg"]
        pbp["assisted_3fg"] = pbp_sub3["assisted_3fg"]
        pbp["assisted_ft"] = pbp_sub3["assisted_ft"]

        pbp["assisted_2fg"].fillna(False, inplace=True)
        pbp["assisted_3fg"].fillna(False, inplace=True)
        pbp["assisted_ft"].fillna(False, inplace=True)

        pbp["assisting_player"] = pbp_sub3["assisting_player"]

        if np.any(pbp["PLAYER_ID"] == pbp["assisting_player"]):
            pass

        pbp["pos"] = pbp["multi_ft"].astype(int) + pbp["2FGA"].astype(int) + pbp["3FGA"].astype(int) + pbp["TO"].astype(
            int) - pbp["O"].astype(int)

        if home:
            self.pbp_processed_home = pbp
        else:
            self.pbp_processed_away = pbp

    def calculate_player_stats(self, home=True):

        pbp = self.pbp_processed_home if home else self.pbp_processed_away
        lineup = self.lineups_home if home else self.lineups_away

        stat_keys = ["AS", "TO", "3FGM", "3FGA", "2FGA", "2FGM",
                     "3FGA", "FTM", "FTA", "D", "O", "REB",
                     "RV", "CM", "FV", "AG", "ST", "OF", "CMT", "CMU", "CMD",
                     "multi_ft", "multi_ft_count", "tech_ft", "assisted_2fg", "assisted_3fg",
                     "assisted_ft", "and_one_2fg", "and_one_3fg", "pos"]

        stat_dict = {x: "sum" for x in stat_keys}

        df = pbp.groupby(["PLAYER_ID", "CODETEAM", "OPP"]).agg(stat_dict).reset_index()
        df = df.loc[df["PLAYER_ID"] != "", :]

        player_df_list = []
        player_df_list_off = []
        for idx, x in enumerate(df["PLAYER_ID"]):
            dfx = lineup.loc[lineup["lineups_string"].str.contains(x), :].copy()
            dfx_off = lineup.loc[~lineup["lineups_string"].str.contains(x), :].copy()

            if dfx.shape[0] > 0:
                dfx["PLAYER_ID"] = x
                dfx["PLAYER_ID"] = x
                dfx_off["PLAYER_ID"] = x
                player_df_list.append(dfx)
                player_df_list_off.append(dfx_off)

        stat_keys_extended = stat_keys + [f"opp_{x}" for x in stat_keys]
        stat_keys_extended.insert(0, "duration")
        stat_dict_extended = {x: "sum" for x in stat_keys_extended}
        stat_keys_off = stat_keys + ["opp_pos", "opp_2FGM", "opp_3FGM", "opp_FTM"]
        stat_dict_off = {x: "sum" for x in stat_keys_off}
        player_df_list = pd.concat(player_df_list, axis=0)
        player_df_list = player_df_list.groupby("PLAYER_ID").agg(stat_dict_extended).reset_index()

        player_df_list_off = pd.concat(player_df_list_off, axis=0)

        player_df_list_off = player_df_list_off.groupby("PLAYER_ID").agg(stat_dict_off).reset_index()

        rename_dict = {x: f"team_{x}" for x in stat_keys}
        player_df_list = player_df_list.rename(columns=rename_dict)
        team_cols = [f"team_{x}" for x in stat_keys]

        rename_dict_off = {x: f"off_{x}" for x in stat_keys_off}
        player_df_list_off = player_df_list_off.rename(columns=rename_dict_off)

        df = df.merge(player_df_list, how="left", on="PLAYER_ID")

        for x, y in zip(stat_keys, team_cols):
            if x == "tech_ft":
                pass
            else:
                df[f"{x}_ratio"] = df[x] / df[y]

        df = df.merge(player_df_list_off, how="left", on="PLAYER_ID")

        df["pts"] = df["FTM"] + 2 * df["2FGM"] + 3 * df["3FGM"]
        df["off_pts"] = df["off_FTM"] + 2 * df["off_2FGM"] + 3 * df["off_3FGM"]
        df["team_pts"] = df["team_FTM"] + 2 * df["team_2FGM"] + 3 * df["team_3FGM"]
        df["opp_pts"] = df["opp_FTM"] + 2 * df["opp_2FGM"] + 3 * df["opp_3FGM"]
        df["plus_minus"] = df["team_pts"] - df["opp_pts"]
        df["game_code"] = self.game_code
        df["home"] = self.home_team == df["CODETEAM"]
        df = df.loc[df["PLAYER_ID"].str.match("^P"), :]
        df["DREBR"] = df["D"] / (df["team_D"] + df["opp_O"])
        df["OREBR"] = df["O"] / (df["team_O"] + df["opp_D"])
        df["usage"] = (df["multi_ft"] + df["2FGA"] + df["3FGA"] + df["TO"]) / (
                df["team_multi_ft"] + df["team_2FGA"] + df["team_3FGA"] + df["team_TO"])

        df["PIR"] = df["pts"] + df["O"] + df["D"] + df["AS"] + df["ST"] + df["FV"] + df["RV"] - df["2FGA"] - df[
            "3FGA"] - \
                    df["FTA"] + df["2FGM"] + df["3FGM"] + df["FTM"] - df["TO"] - df["AG"] - df["CM"] - df["OF"] - \
                    df["CMT"] - df["CMU"] - df["CMD"]

        df["on_ORtg"] = 100 * (df["team_pts"] / df["team_pos"])
        df["off_ORtg"] = 100 * (df["off_pts"] / df["off_pos"])
        df["on_DRtg"] = 100 * (df["opp_pts"] / df["opp_pos"])
        df["off_opp_pts"] = df["off_opp_FTM"] + 2 * df["off_opp_2FGM"] + 3 * df["off_opp_3FGM"]
        df["off_DRtg"] = 100 * (df["off_opp_pts"] / df["off_opp_pos"])

        df["eFG"] = (df["pts"] - df["FTM"]) / (2 * df["2FGA"] + 2 * df["3FGA"])

        # calculate individual ORTG and DRTG

        q_ast = ((df["assisted_2fg"] + df["assisted_3fg"]) / (
                df["2FGM"] + df["3FGM"])).fillna(0)

        pts_gen_fg = ((df["2FGM"] + df["3FGM"]) * (1 - (0.5 * df["eFG"] * q_ast))).fillna(0)

        pts_gen_ast = (0.5 * df["AS"]) * ((
                                                  (df["team_pts"] - df["team_FTM"]) - (
                                                  df["pts"] - df["FTM"])) / (
                                                  2 * (df["team_2FGA"] + df["team_3FGA"] -
                                                       df["2FGA"] - df["3FGA"])))

        pts_gen_ft = ((1 - np.square(1 - df["FTM"] / df["FTA"])) * df["multi_ft"]).fillna(0)

        team_scoring_poss = df["team_2FGM"] + df["team_3FGM"] + (1 - np.square(1 - df["team_FTM"] / df["team_FTA"])) * \
                            df["team_multi_ft"]

        team_orb_pct = df["team_O"] / (df["team_O"] + df["opp_D"])
        team_play_pct = team_scoring_poss / (df["team_2FGA"] + df["team_3FGA"] + df["team_multi_ft"] + df["team_TO"])
        team_orb_weight = ((1 - team_orb_pct) * team_play_pct) / (
                (1 - team_orb_pct) * team_play_pct + team_orb_pct * (1 - team_play_pct))

        pts_gen_orb = df["O"] * team_orb_weight * team_play_pct

        scoring_poss = (pts_gen_fg + pts_gen_ast + pts_gen_ft.fillna(0)) * \
                       (1 - (df["team_O"] / team_scoring_poss) * team_orb_weight * team_play_pct) + pts_gen_orb

        missed_fg_part = (df["2FGA"] + df["3FGA"] - df["2FGM"] - df["3FGM"]) * (1 - 1.07 * team_orb_pct)

        missed_ft_part = 1 - pts_gen_ft

        ind_pos = scoring_poss + missed_ft_part + missed_fg_part + df["TO"]

        pts_prod_fg = ((2 * df["2FGM"] + 3 * (df["3FGM"])) * (
                1 - 0.5 * ((df["pts"] - df["FTM"]) / (2 * (df["2FGA"] + df["3FGA"]))
                           ) * q_ast)).fillna(0)

        pts_prod_ast = (((2 * (df["team_2FGM"] - df["2FGM"]) +
                          3 * (df["team_3FGM"] - df["3FGM"])
                          ) / (
                                 df["team_3FGM"] + df["team_2FGM"] -
                                 df["2FGM"] - df["3FGM"])) * pts_gen_ast).fillna(0)

        pts_prod_ft = df["FTM"]

        pts_prod_orb = df["O"] * team_orb_weight * team_play_pct * (df["team_pts"] / team_scoring_poss)

        points_produced = (pts_prod_fg + pts_prod_ast + pts_prod_ft) * \
                          (1 - (df["team_O"] / team_scoring_poss) * team_orb_weight * team_play_pct) + pts_prod_orb

        df["ORtg"] = 100 * (points_produced / ind_pos)

        ft_test = (df["multi_ft_count"] + df["and_one_2fg"] + df["tech_ft"] + df["and_one_3fg"] - df["FTA"])
        if ~np.all(ft_test == 0):
            pass

        if home:
            self.home_players_processed = df
        else:
            self.away_players_processed = df

    def calculate_team_stats(self):
        home_stats = self.lineups_home.drop("lineups_string", axis=1).groupby(
            ["CODETEAM", "OPP"]).agg(sum).reset_index()
        away_stats = self.lineups_away.drop("lineups_string", axis=1).groupby(
            ["CODETEAM", "OPP"]).agg(sum).reset_index()

        home_stats["points_scored"] = home_stats["FTM"] + 2 * home_stats["2FGM"] + 3 * home_stats["3FGM"]
        away_stats["points_scored"] = away_stats["FTM"] + 2 * away_stats["2FGM"] + 3 * away_stats["3FGM"]
        home_stats["opp_points_scored"] = away_stats["points_scored"]
        away_stats["opp_points_scored"] = home_stats["points_scored"]

        home_stats["win"] = (home_stats["points_scored"] > home_stats["opp_points_scored"]).astype(int)
        away_stats["win"] = (away_stats["points_scored"] > away_stats["opp_points_scored"]).astype(int)

        home_stats["home"] = True
        away_stats["home"] = False

        home_stats["2FGR"] = home_stats["2FGM"] / home_stats["2FGA"]
        home_stats["3FGR"] = home_stats["3FGM"] / home_stats["3FGA"]
        home_stats["FTR"] = home_stats["FTM"] / home_stats["FTA"]
        home_stats["DRBEBR"] = home_stats["D"] / (home_stats["D"] + home_stats["opp_O"])
        home_stats["ORBEBR"] = home_stats["O"] / (home_stats["O"] + home_stats["opp_D"])
        home_stats["PPP"] = home_stats["points_scored"] / home_stats["pos"]
        home_stats["TOR"] = home_stats["TO"] / (
                home_stats["2FGA"] + home_stats["3FGA"] + home_stats["multi_ft"] + home_stats["TO"])
        home_stats["FT_four"] = home_stats["FTM"] / (
                home_stats["2FGA"] + home_stats["3FGA"])

        home_stats["team_name"] = self.home_team_name

        away_stats["2FGR"] = away_stats["2FGM"] / away_stats["2FGA"]
        away_stats["3FGR"] = away_stats["3FGM"] / away_stats["3FGA"]
        away_stats["FTR"] = away_stats["FTM"] / away_stats["FTA"]
        away_stats["DRBEBR"] = away_stats["D"] / (away_stats["D"] + away_stats["opp_O"])
        away_stats["ORBEBR"] = away_stats["O"] / (away_stats["O"] + away_stats["opp_D"])
        away_stats["PPP"] = away_stats["points_scored"] / away_stats["pos"]
        away_stats["TOR"] = away_stats["TO"] / (
                away_stats["2FGA"] + away_stats["3FGA"] + away_stats["multi_ft"] + away_stats["TO"])
        away_stats["FT_four"] = away_stats["FTM"] / (away_stats["2FGA"] + away_stats["3FGA"])

        away_stats["team_name"] = self.away_team_name

        self.team_stats = pd.concat([home_stats, away_stats])
        self.team_stats["game_code"] = self.game_code

    def get_assist_data(self, home=True):
        df = self.pbp_processed_home if home else self.pbp_processed_away
        df_assists = df.loc[
            ~df["assisting_player"].isna(), ["PLAYER_ID", "assisting_player", "CODETEAM", "PLAYTYPE", "time"]]
        df_assists["OPP"] = self.away_team if home else self.home_team
        df_assists["game_code"] = self.game_code
        df_assists["season"] = self.season
        df_assists.loc[:, "assisting_player"] = df_assists["assisting_player"].str.replace(" ", "")
        df_assists["home"] = home

        if home:
            self.assists_home = df_assists
        else:
            self.assists_away = df_assists

        pass

    def replace_player_ids(self):

        home_dict = {idx: name for idx, name in zip(self.home_players["ac"], self.home_players["na"])}
        away_dict = {idx: name for idx, name in zip(self.away_players["ac"], self.away_players["na"])}

        self.home_players_processed["playerName"] = self.home_players_processed["PLAYER_ID"].map(home_dict).str.title()
        self.away_players_processed["playerName"] = self.away_players_processed["PLAYER_ID"].map(away_dict).str.title()

        self.lineups_home["lineups"] = self.lineups_home["lineups_string"].replace(home_dict, regex=True).str.title()
        self.lineups_away["lineups"] = self.lineups_away["lineups_string"].replace(away_dict, regex=True).str.title()

        self.assists_home["playerName"] = self.assists_home["PLAYER_ID"].map(home_dict).str.title()
        self.assists_away["playerName"] = self.assists_away["PLAYER_ID"].map(away_dict).str.title()

        self.assists_home["playerNameAssisting"] = self.assists_home["assisting_player"].map(home_dict).str.title()
        self.assists_away["playerNameAssisting"] = self.assists_away["assisting_player"].map(away_dict).str.title()

        pass

    def add_player_info(self):

        self.home_players_processed = self.home_players_processed.merge(
            self.home_players[["ac", "p", "im"]],
            how="left",
            left_on="PLAYER_ID",
            right_on="ac")
        self.away_players_processed = self.away_players_processed.merge(
            self.away_players[["ac", "p", "im"]],
            how="left",
            left_on="PLAYER_ID",
            right_on="ac")

        pass

    def process_game_data(self):

        self.extract_home_away_lineups()

        self.stat_calculator()
        self.stat_calculator(home=False)

        self.extra_stats_finder()
        self.extra_stats_finder(home=False)

        self.opp_stat_calculator()
        self.opp_stat_calculator(home=False)

        self.calculate_player_stats()
        self.calculate_player_stats(home=False)

        self.calculate_team_stats()

        self.get_points_data()

        self.get_assist_data()
        self.get_assist_data(home=False)

        self.replace_player_ids()
        self.add_player_info()


@dataclass
class SeasonData:
    game_list: list
    season: int
    player_data: pd.DataFrame
    lineup_data: pd.DataFrame
    team_data: pd.DataFrame
    player_data_agg: pd.DataFrame
    lineup_data_agg: pd.DataFrame
    team_data_agg: pd.DataFrame
    points_data: pd.DataFrame
    assists_data: pd.DataFrame
    quantiles: pd.DataFrame

    def __init__(self, season):
        self.season = season
        self.game_list = []

    def store_games_list(self, game_list):
        self.game_list = [GameData.from_dict(x) for x in game_list]

    def concatenate_player_data(self):
        player_data_list = [x.home_players_processed for x in self.game_list] + [x.away_players_processed for x in
                                                                                 self.game_list]
        self.player_data = pd.concat(player_data_list)
        self.player_data["season"] = self.season

    def concatenate_lineup_data(self):
        lineup_data_list = [x.lineups_home for x in self.game_list] + [x.lineups_away for x in
                                                                       self.game_list]
        self.lineup_data = pd.concat(lineup_data_list)
        self.lineup_data["season"] = self.season
        pass

    def concatenate_team_data(self):
        team_data_list = [x.team_stats for x in self.game_list]
        self.team_data = pd.concat(team_data_list)
        self.team_data["season"] = self.season

        pass

    def concatenate_points_data(self):
        points_data_list = [x.points for x in self.game_list]
        self.points_data = pd.concat(points_data_list)

    def concatenate_assists_data(self):
        assists_data_list = [x.assists_home for x in self.game_list] + [x.assists_away for x in self.game_list]
        self.assists_data = pd.concat(assists_data_list)

    def concatenate_quantile_data(self):
        percentiles = [0.25, 0.50, 0.75, 0.95, 1]

        player_quantiles = [self.player_data["PIR"].quantile(x) for x in percentiles]
        team_quantiles = [self.team_data["PPP"].quantile(x) for x in percentiles]

        df = pd.DataFrame.from_dict({0: [player_quantiles, "player"],
                                     1: [team_quantiles, "team"]},
                                    orient="index").rename(columns={0: "quantiles",
                                                                    1: "type"})
        self.quantiles = df

    def aggregate_player_data(self):
        self.player_data["game_count"] = 1
        numeric_columns = self.player_data.select_dtypes(include=np.number).columns.tolist()

        cols_to_average = [x for x in numeric_columns if re.search("_ratio", x)]
        cols_to_sum = [x for x in numeric_columns if not re.search("_ratio", x)]

        cols_dict = {x: "sum" for x in cols_to_sum} | {x: "mean" for x in cols_to_average}

        self.player_data_agg = self.player_data.groupby(["PLAYER_ID", "playerName", "CODETEAM", "p", "im"]).agg(
            cols_dict)
        self.player_data_agg = self.player_data_agg.reset_index()
        self.player_data_agg["season"] = self.season

        df_averages = pd.DataFrame({
            f"{x}_avg": self.player_data_agg[x] / self.player_data_agg["game_count"] for x in cols_to_sum})

        self.player_data_agg = self.player_data_agg.join(df_averages)
        self.player_data_agg["2FGP"] = self.player_data_agg["2FGM"] / self.player_data_agg["2FGA"]
        self.player_data_agg["3FGR"] = self.player_data_agg["3FGM"] / self.player_data_agg["3FGA"]
        self.player_data_agg["FTR"] = self.player_data_agg["FTM"] / self.player_data_agg["FTA"]

        self.player_data_agg["DREBR"] = self.player_data_agg["D"] / \
                                        (self.player_data_agg["team_D"] + self.player_data_agg["opp_O"])
        self.player_data_agg["OREBR"] = self.player_data_agg["O"] / \
                                        (self.player_data_agg["team_O"] + self.player_data_agg["opp_D"])
        self.player_data_agg["usage"] = (self.player_data_agg["multi_ft"] +
                                         self.player_data_agg["2FGA"] +
                                         self.player_data_agg["3FGA"] +
                                         self.player_data_agg["TO"]) / (
                                                self.player_data_agg["team_multi_ft"] +
                                                self.player_data_agg["team_2FGA"] +
                                                self.player_data_agg["team_3FGA"] +
                                                self.player_data_agg["team_TO"])
        self.player_data_agg["a2Pr"] = self.player_data_agg["assisted_2fg"] / self.player_data_agg["2FGM"]
        self.player_data_agg["a3Pr"] = self.player_data_agg["assisted_3fg"] / self.player_data_agg["3FGM"]

        self.player_data_agg["eFG"] = (self.player_data_agg["pts"] - self.player_data_agg["FTM"]) / \
                                      (2 * self.player_data_agg["2FGA"] + 2 * self.player_data_agg["3FGA"])

        self.player_data_agg["TS"] = self.player_data_agg["pts"] / (
                2 * (self.player_data_agg["2FGA"] + self.player_data_agg["3FGA"]) + self.player_data_agg["multi_ft"])

    def aggregate_lineup_data(self):
        self.lineup_data["game_count"] = 1
        numeric_columns = self.lineup_data.select_dtypes(include=np.number).columns.tolist()

        cols_to_sum = [x for x in numeric_columns]
        cols_dict = {x: "sum" for x in cols_to_sum}

        self.lineup_data_agg = self.lineup_data.groupby(["lineups_string", "lineups", "CODETEAM"]).agg(cols_dict)
        self.lineup_data_agg = self.lineup_data_agg.reset_index()

        df_averages = pd.DataFrame({
            f"{x}_avg": self.lineup_data_agg[x] / self.lineup_data_agg["game_count"] for x in cols_to_sum})

        self.lineup_data_agg = self.lineup_data_agg.join(df_averages)
        self.lineup_data_agg["2FGR"] = self.lineup_data_agg["2FGM"] / self.lineup_data_agg["2FGA"]
        self.lineup_data_agg["3FGR"] = self.lineup_data_agg["3FGM"] / self.lineup_data_agg["3FGA"]
        self.lineup_data_agg["FTR"] = self.lineup_data_agg["FTM"] / self.lineup_data_agg["FTA"]

        self.lineup_data_agg["season"] = self.season

        pass

    def aggregate_team_data(self):
        self.team_data["game_count"] = 1
        numeric_columns = self.team_data.select_dtypes(include=np.number).columns.tolist()
        cols_to_sum = [x for x in numeric_columns]
        cols_dict = {x: "sum" for x in cols_to_sum}
        self.team_data_agg = self.team_data.groupby(["CODETEAM", "team_name"]).agg(cols_dict)
        self.team_data_agg = self.team_data_agg.reset_index()

        df_averages = pd.DataFrame({
            f"{x}_avg": self.team_data_agg[x] / self.team_data_agg["game_count"] for x in cols_to_sum})

        self.team_data_agg = self.team_data_agg.join(df_averages)
        self.team_data_agg["2FGR"] = self.team_data_agg["2FGM"] / self.team_data_agg["2FGA"]
        self.team_data_agg["3FGR"] = self.team_data_agg["3FGM"] / self.team_data_agg["3FGA"]
        self.team_data_agg["FTR"] = self.team_data_agg["FTM"] / self.team_data_agg["FTA"]
        self.team_data_agg["FG"] = (self.team_data_agg["2FGM"] + self.team_data_agg["3FGM"]) / \
                                   (self.team_data_agg["2FGA"] + self.team_data_agg["3FGA"])
        self.team_data_agg["season"] = self.season
        self.team_data_agg["ORtg"] = 100 * self.team_data_agg["points_scored"] / self.team_data_agg["pos"]
        self.team_data_agg["DRtg"] = 100 * self.team_data_agg["opp_points_scored"] / self.team_data_agg["opp_pos"]

        self.team_data_agg["TOR"] = self.team_data_agg["TO"] / (
                self.team_data_agg["2FGA"] + self.team_data_agg["3FGA"] + self.team_data_agg["multi_ft"] +
                self.team_data_agg["TO"])
        self.team_data_agg["FT_four"] = self.team_data_agg["FTM"] / (
                self.team_data_agg["2FGA"] + self.team_data_agg["3FGA"])
        self.team_data_agg["eFG"] = (self.team_data_agg["points_scored"] -
                                     self.team_data_agg["FTM"]) / (
                                            2 * (self.team_data_agg["2FGA"] + self.team_data_agg["3FGA"]))

        self.team_data_agg["pace"] = (self.team_data_agg["pos"] +
                                      self.team_data_agg["opp_pos"]) / (2 * self.team_data_agg["game_count"])

        self.team_data_agg["DREBR"] = self.team_data_agg["D"] / (self.team_data_agg["opp_O"] + self.team_data_agg["D"])
        self.team_data_agg["OREBR"] = self.team_data_agg["O"] / (self.team_data_agg["opp_D"] + self.team_data_agg["O"])

    def calculate_per_game_based(self):
        league_vop = np.sum(self.team_data_agg["points_scored"]) / np.sum(self.team_data_agg["pos"])
        league_drp = np.sum(self.team_data_agg["D"]) / np.sum(self.team_data_agg["D"] + self.team_data_agg["O"])
        league_factor = (2 / 3) - np.sum(self.team_data_agg["AS"] / (2 * np.sum(self.team_data_agg["2FGM"] +
                                                                                np.sum(self.team_data_agg["3FGM"]))) / (
                                                 2 * (np.sum(self.team_data_agg["2FGM"]) +
                                                      np.sum(self.team_data_agg["3FGM"])) / np.sum(
                                             self.team_data_agg["FTM"])))
        league_foul = np.sum(self.team_data_agg["FTM"] -
                             (league_vop * self.team_data_agg["multi_ft"])) / np.sum(
            self.team_data_agg["CM"] + self.team_data_agg["CMU"] + self.team_data_agg["OF"] + self.team_data_agg["CMT"])

        league_pace = np.mean(
            (self.team_data_agg["pos"] + self.team_data_agg["opp_pos"]) / self.team_data_agg["game_count"] / 2)

        # TODO merging could be faster
        def u_per(row):
            team_stats = self.team_data.loc[
                         (self.team_data["game_code"] == row["game_code"]) &
                         (self.team_data["CODETEAM"] == row["CODETEAM"]), :]

            fg = row["2FGM"] + row["3FGM"]
            fga = row["2FGA"] + row["3FGA"]
            team_fg = team_stats["2FGM"].iloc[0] + team_stats["3FGM"].iloc[0]
            c0 = (1 / (row["duration"] / 60))
            c1 = row["3FGM"] + 0.66 * row["AS"]
            c2 = (2 - league_factor * team_stats["AS"] / team_fg) * fg
            c3_1 = 0.5 * row["FTM"]
            c3_2 = 2 - team_stats["AS"] / (team_fg * 3)
            c4 = league_vop * row["TO"]
            c5 = league_vop * league_drp * (fga - fg)
            c6 = league_vop * 0.44 * (0.44 + (0.56 * league_drp)) * (row["FTA"] - row["FTM"])
            c7 = league_vop * (1 - league_drp) * row["D"]
            c8 = league_vop * league_drp * row["O"]
            c9 = league_vop * row["ST"]
            c10 = league_vop * league_drp * row["FV"]
            c11 = league_foul * row["CM"]

            uper_stat = c0 * (c1 + c2 + (c3_1 * c3_2) - c4 - c5 - c6 + c7 + c8 + c9 + c10 - c11)
            team_pace = (team_stats["pos"].iloc[0] + team_stats["opp_pos"].iloc[0]) / 2

            return uper_stat.iloc[0], team_pace

        df_tmp = self.player_data.apply(lambda x: u_per(x), axis=1, result_type="expand")
        df_tmp.columns = ["uPER", "team_pace"]

        self.player_data["uPER"] = df_tmp["uPER"]
        self.player_data["PER"] = (df_tmp["uPER"] * league_pace / df_tmp["team_pace"]) * 15 / np.mean(
            self.player_data.loc[self.player_data["duration"] >= 180, "uPER"])
        self.player_data.loc[self.player_data["duration"] < 180, "PER"] = np.nan

    def calculate_per_season_based(self):
        league_vop = np.sum(self.team_data_agg["points_scored"]) / np.sum(self.team_data_agg["pos"])
        league_drp = np.sum(self.team_data_agg["D"]) / np.sum(self.team_data_agg["D"] + self.team_data_agg["O"])
        league_factor = (2 / 3) - np.sum(self.team_data_agg["AS"] / (2 * np.sum(self.team_data_agg["2FGM"] +
                                                                                np.sum(self.team_data_agg["3FGM"]))) / (
                                                 2 * (np.sum(self.team_data_agg["2FGM"]) +
                                                      np.sum(self.team_data_agg["3FGM"])) / np.sum(
                                             self.team_data_agg["FTM"])))
        league_foul = np.sum(self.team_data_agg["FTM"] -
                             (league_vop * self.team_data_agg["multi_ft"])) / np.sum(
            self.team_data_agg["CM"] + self.team_data_agg["CMU"] + self.team_data_agg["OF"] + self.team_data_agg["CMT"])

        league_pace = np.mean(
            (self.team_data_agg["pos"] + self.team_data_agg["opp_pos"]) / self.team_data_agg["game_count"] / 2)

        # TODO merging could be faster
        def u_per(row):
            team_stats = self.team_data_agg.loc[(self.team_data_agg["CODETEAM"] == row["CODETEAM"]), :]

            fg = row["2FGM"] + row["3FGM"]
            fga = row["2FGA"] + row["3FGA"]
            team_fg = (team_stats["2FGM"] + team_stats["3FGM"]).iloc[0]
            c0 = (1 / (row["duration"] / 60))
            c1 = row["3FGM"] + 0.66 * row["AS"]
            c2 = (2 - league_factor * team_stats["AS"].iloc[0] / team_fg) * fg
            c3_1 = 0.5 * row["FTM"]
            c3_2 = 2 - team_stats["AS"].iloc[0] / (team_fg * 3)
            c4 = league_vop * row["TO"]
            c5 = league_vop * league_drp * (fga - fg)
            c6 = league_vop * 0.44 * (0.44 + (0.56 * league_drp)) * (row["FTA"] - row["FTM"])
            c7 = league_vop * (1 - league_drp) * row["D"]
            c8 = league_vop * league_drp * row["O"]
            c9 = league_vop * row["ST"]
            c10 = league_vop * league_drp * row["FV"]
            c11 = league_foul * (row["CM"] + row["OF"] + row["CMU"])

            uper_stat = c0 * (c1 + c2 + (c3_1 * c3_2) - c4 - c5 - c6 + c7 + c8 + c9 + c10 - c11)
            team_pace = team_stats["pos"].iloc[0] / team_stats["game_count"].iloc[0]

            return uper_stat, team_pace

        df_tmp = self.player_data_agg.apply(lambda x: u_per(x), axis=1, result_type="expand")
        df_tmp.columns = ["uPER_season", "team_pace"]

        self.player_data_agg["uPER_season"] = df_tmp["uPER_season"]
        self.player_data_agg["PER_season"] = (df_tmp["uPER_season"] * league_pace / df_tmp["team_pace"]) * 15 / np.mean(
            self.player_data_agg.loc[self.player_data_agg["duration"] >= 1800, "uPER_season"])
        self.player_data_agg.loc[self.player_data_agg["duration"] < 1800, "PER_season"] = np.nan
        return df_tmp

    def get_percentile_ranks(self):
        self.player_data_agg["PIR_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "PIR_avg"].rank(pct=True) * 100
        self.player_data_agg["PPG_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "pts_avg"].rank(pct=True) * 100
        self.player_data_agg["PER_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "PER_season"].rank(
            pct=True) * 100
        self.player_data_agg["EFG_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "eFG"].rank(pct=True) * 100
        self.player_data_agg["MPG_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "duration_avg"].rank(
            pct=True) * 100
        self.player_data_agg["USG_rank"] = self.player_data_agg.loc[
                                               self.player_data_agg["duration"] >= 1800, "usage"].rank(pct=True) * 100

        self.team_data_agg["ORtg_rank"] = self.team_data_agg["ORtg"].rank(ascending=False).astype(int)
        self.team_data_agg["DRtg_rank"] = self.team_data_agg["DRtg"].rank().astype(int)
        self.team_data_agg["FG_rank"] = self.team_data_agg["FG"].rank(ascending=False).astype(int)
        self.team_data_agg["pace_rank"] = self.team_data_agg["pace"].rank(ascending=False).astype(int)
        self.team_data_agg["2FGR_rank"] = self.team_data_agg["2FGR"].rank(ascending=False).astype(int)
        self.team_data_agg["3FGR_rank"] = self.team_data_agg["3FGR"].rank(ascending=False).astype(int)
        self.team_data_agg["TOR_rank"] = self.team_data_agg["TOR"].rank().astype(int)
        self.team_data_agg["AS_rank"] = self.team_data_agg["AS_avg"].rank(ascending=False).astype(int)
        self.team_data_agg["DREB_rank"] = self.team_data_agg["DREBR"].rank(ascending=False).astype(int)
        self.team_data_agg["OREB_rank"] = self.team_data_agg["OREBR"].rank(ascending=False).astype(int)
        self.team_data_agg["FT_rank"] = self.team_data_agg["FT_four"].rank(ascending=False).astype(int)
        self.team_data_agg["as2P"] = self.team_data_agg["assisted_2fg"] / self.team_data_agg["2FGM"]
        self.team_data_agg["as3P"] = self.team_data_agg["assisted_3fg"] / self.team_data_agg["3FGM"]

    def aggregate_quantile_data(self):
        percentiles = [0.25, 0.50, 0.75, 0.95, 1]

        player_quantiles = [self.player_data_agg["PIR_avg"].quantile(x) for x in percentiles]

        df = pd.DataFrame.from_dict({0: [player_quantiles, "player_agg"]
                                     },
                                    orient="index").rename(columns={0: "quantiles",
                                                                    1: "type"})
        self.quantiles = pd.concat([self.quantiles, df])
