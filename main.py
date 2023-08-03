import json
from tqdm.auto import tqdm

import processing.game_data
from processing.db_connection import MongoConnectionSeason
from processing.game_data import SeasonData
from el_api_wrapper import ELAPI
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("season", help="euroleague season to process", type=int)
parser.add_argument("--download", help="flag to tell if the season data should be downloaded, looks for a file in dir "
                                       "otherwise", action="store_true")
args = parser.parse_args()

if __name__ == '__main__':
    el = ELAPI()

    loading = not args.download
    season = args.season
    season_instance = SeasonData(season)

    if loading:

        with open(f"season_{season}.json", "r") as file:
            season_game_list = json.load(file)

    else:
        season_game_list = el.get_season_stats(season_instance.season)
        with open(f"season_{season}.json", "w") as file:
            json.dump(season_game_list, file)

    season_instance.store_games_list(season_game_list)

    pbar = tqdm(total=len(season_game_list))
    pbar.set_description("Processing Game Data:")
    i = 0
    while i < len(season_instance.game_list):
        season_instance.game_list[i].process_game_data()
        i += 1
        pbar.update(1)
    pbar.close()

    season_instance.concatenate_lineup_data()
    season_instance.concatenate_team_data()
    season_instance.concatenate_player_data()
    season_instance.concatenate_points_data()
    season_instance.concatenate_assists_data()
    season_instance.concatenate_quantile_data()

    season_instance.aggregate_lineup_data()
    season_instance.aggregate_team_data()
    season_instance.aggregate_player_data()
    season_instance.aggregate_quantile_data()

    season_instance.calculate_per_game_based()
    season_instance.calculate_per_season_based()
    season_instance.get_percentile_ranks()

    conn = MongoConnectionSeason(season_instance)
    conn.insert_season()
