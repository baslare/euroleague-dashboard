import json

import processing.processing_functions
from processing.game_data import GameData
from el_api_wrapper import ELAPI

if __name__ == '__main__':
    el = ELAPI()
    #response = el.get_season_stats(2022)
    response = el.get_game_stats(2022, 1)
    ls = processing.processing_functions.make_pbp_df(response.get("play_by_play"))
    ls.to_csv('game1.csv')

    with open("game1.json", "w") as file:
        json.dump(response, file)

    gd: GameData
    gd = GameData.from_dict(response)
    print(gd.points)




