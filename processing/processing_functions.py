import pandas as pd
import numpy as np


def make_pbp_df(pbp_data: dict) -> pd.DataFrame:

    pbp_quarters = {key: val for key, val in pbp_data.items() if type(val) == list}
    pbp_quarters = {key: pd.DataFrame(val) for key, val in pbp_quarters.items()}

    pbp_quarters = pd.concat(pbp_quarters).reset_index()
    pbp_quarters = pbp_quarters[["CODETEAM", "PLAYER_ID", "PLAYTYPE", "PLAYER", "MARKERTIME", "MINUTE"]]

    pbp_quarters["MARKERTIME"][0] = "09:60"
    pbp_quarters["PLAYTYPE"][0] = "BG"

    pbp_quarters.loc[len(pbp_quarters["MARKERTIME"]) - 1, "MARKERTIME"] = "00:00"
    pbp_quarters["MINUTE"][len(pbp_quarters["MINUTE"]) - 1] = pbp_quarters["MINUTE"][len(pbp_quarters["MINUTE"]) - 1] - 1

    pbp_quarters["Quarter"] = np.where(pbp_quarters["MINUTE"] <= 40, np.ceil(pbp_quarters["MINUTE"] / 10),
                                       np.where(pbp_quarters["MINUTE"] <= 45, 5,
                                                np.where(pbp_quarters["MINUTE"] <= 50, 6,
                                                         np.where(pbp_quarters["MINUTE"] <= 55, 7, 8))))

    pbp_quarters[["min", "sec"]] = pbp_quarters["MARKERTIME"].str.split(":", expand=True)

    pbp_quarters['time'] = (pbp_quarters["MINUTE"] - 1)*60 + (60 - pbp_quarters["sec"].fillna("00").astype(int))

    pbp_quarters = pbp_quarters.loc[~pbp_quarters["PLAYTYPE"].isin(["BP", "EP"]), :]

    pbp_quarters["playerIn"] = pbp_quarters["PLAYTYPE"] == 'IN'
    pbp_quarters["playerOut"] = pbp_quarters["PLAYTYPE"] == 'OUT'
    pbp_quarters["CODETEAM"] = pbp_quarters["CODETEAM"].str.replace(" ", "")
    pbp_quarters["PLAYER_ID"] = pbp_quarters["PLAYER_ID"].str.replace(" ",  "")

    return pbp_quarters
