import json
import pymongo
from tqdm.auto import tqdm
import processing.processing_functions
from processing.game_data import SeasonData
from el_api_wrapper import ELAPI

if __name__ == '__main__':
    el = ELAPI()
    # season_instance = SeasonData(2022)
    # season_game_list = el.get_season_stats(season_instance.season)

    # with open("season.json", "w") as file:
    # json.dump(season_game_list, file)

    with open("season.json", "r") as file:
        season_game_list = json.load(file)

    season_instance = SeasonData(2022)
    season_instance.store_games_list(season_game_list)

    pbar = tqdm(total=len(season_game_list))
    pbar.set_description("Processing Game Data:")
    i = 0
    while i < len(season_instance.game_list):
        season_instance.game_list[i] = season_instance.game_list[i].process_game_data()
        i += 1
        pbar.update(1)
    pbar.close()

    season_instance.concatenate_lineup_data()
    season_instance.concatenate_team_data()
    season_instance.concatenate_player_data()

    client = pymongo.MongoClient("mongodb://root:password@mongo:27017/")

    db = client["euroleague_dashboard"]
    lineups = db["lineups"]
    lineups.insert_many(season_instance.lineup_data.to_dict("records"))

    players = db["players"]
    players.insert_many(season_instance.player_data.to_dict("records"))

    teams = db["teams"]
    teams.insert_many(season_instance.team_data.to_dict("records"))
