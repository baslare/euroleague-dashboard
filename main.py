import json

import processing.processing_functions
from processing.game_data import GameData
from el_api_wrapper import ELAPI

if __name__ == '__main__':
    el = ELAPI()
    #response = el.get_season_stats(2022)
    #response = el.get_game_stats(2022, 1)
    #ls = processing.processing_functions.make_pbp_df(response.get("play_by_play"))
    #ls.to_csv('game1.csv')

    with open("game1.json", "r") as file:
        response = json.load(file)

    gd: GameData
    gd = GameData.from_dict(response)

    gd.extract_home_away_lineups()

    gd.stat_calculator()
    gd.stat_calculator(home=False)

    gd.extra_stats_finder()
    gd.extra_stats_finder(home=False)

    gd.opp_stat_calculator()
    gd.opp_stat_calculator(home=False)

    gd.get_player_stats()

    # gd.pbp_processed_home.to_csv("pbp1.csv")
    # gd.pbp_processed_away.to_csv("pbp2.csv")

    # gd.lineups_home.to_csv("lineup1.csv")
    # gd.lineups_away.to_csv("lineup2.csv")










