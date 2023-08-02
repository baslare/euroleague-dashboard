import pandas as pd
import numpy as np


def make_pbp_df(pbp_data: dict) -> pd.DataFrame:
    pbp_quarters = {key: val for key, val in pbp_data.items() if type(val) == list}
    pbp_quarters = {key: pd.DataFrame(val) for key, val in pbp_quarters.items()}

    pbp_quarters = pd.concat(pbp_quarters).reset_index()
    pbp_quarters = pbp_quarters[["CODETEAM", "PLAYER_ID", "PLAYTYPE", "PLAYER", "MARKERTIME", "MINUTE"]]

    pbp_quarters.loc[0, "MARKERTIME"] = "10:00"
    pbp_quarters.loc[pbp_quarters.shape[0] - 1, "MARKERTIME"] = "00:00"
    pbp_quarters.loc[0, "PLAYTYPE"] = "BG"
    pbp_quarters = pbp_quarters.loc[(pbp_quarters["PLAYTYPE"] != "BP") & (pbp_quarters["PLAYTYPE"] != "EP"), :]

    pbp_quarters.loc[len(pbp_quarters["MARKERTIME"]) - 1, "MARKERTIME"] = "00:00"
    pbp_quarters["MINUTE"].iloc[-1] = pbp_quarters["MINUTE"].iloc[-1] - 1

    pbp_quarters["Quarter"] = np.where(pbp_quarters["MINUTE"] <= 40, np.ceil(pbp_quarters["MINUTE"] / 10),
                                       np.where(pbp_quarters["MINUTE"] <= 45, 5,
                                                np.where(pbp_quarters["MINUTE"] <= 50, 6,
                                                         np.where(pbp_quarters["MINUTE"] <= 55, 7, 8))))

    pbp_quarters = pbp_quarters.loc[pbp_quarters["MARKERTIME"].str.contains(":"), :]
    pbp_quarters[["min", "sec"]] = pbp_quarters["MARKERTIME"].str.split(":", expand=True)

    pbp_quarters["min"] = pbp_quarters["min"].astype(int)
    pbp_quarters["sec"] = pbp_quarters["sec"].fillna("00").astype(int)

    pbp_quarters['time'] = np.where(pbp_quarters["Quarter"] <= 4,
                                    (pbp_quarters["Quarter"]) * 600 - pbp_quarters["min"] * 60 - pbp_quarters["sec"],
                                    2400 + (pbp_quarters["Quarter"] - 4) * 300 - (
                                            pbp_quarters["min"] * 60 + pbp_quarters["sec"]))

    pbp_quarters = pbp_quarters.sort_values("time")

    pbp_quarters = pbp_quarters.loc[~pbp_quarters["PLAYTYPE"].isin(["BP", "EP"]), :]

    pbp_quarters["playerIn"] = pbp_quarters["PLAYTYPE"] == 'IN'
    pbp_quarters["playerOut"] = pbp_quarters["PLAYTYPE"] == 'OUT'
    pbp_quarters["CODETEAM"] = pbp_quarters["CODETEAM"].str.replace(" ", "")
    pbp_quarters["PLAYER_ID"] = pbp_quarters["PLAYER_ID"].str.replace(" ", "")
    pbp_quarters["PLAYTYPE"] = pbp_quarters["PLAYTYPE"].str.replace(" ", "")
    pbp_quarters.loc[pbp_quarters["playerIn"], "PLAYTYPE"] = "ZZ IN"
    pbp_quarters.loc[pbp_quarters["playerOut"], "PLAYTYPE"] = "ZZ OUT"

    return pbp_quarters.reset_index(drop=True)


def make_players_df(players_data: list):
    return pd.DataFrame.from_records(players_data).reset_index(drop=True)


def make_points_df(points: dict):
    return pd.DataFrame.from_records(points.get("Rows"))


