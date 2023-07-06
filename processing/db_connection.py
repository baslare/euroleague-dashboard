import pymongo
from processing.game_data import SeasonData, GameData


class MongoConnectionGame:
    client: pymongo.MongoClient

    def __init__(self, obj: GameData):
        self.client = pymongo.MongoClient("mongodb://root:password@mongo:27017/")
        self.db = self.client["euroleague_dashboard"]

        self.game_data = obj

        self.lineups = self.db["lineups"]
        self.players = self.db["players"]
        self.teams = self.db["teams"]

    def insert_single_game(self):
        self.lineups.insert_many(self.game_data.lineup_data.to_dict("records"))
        self.players.insert_many(self.game_data.home_players_processed.to_dict("records"))
        self.players.insert_many(self.game_data.away_players_processed.to_dict("records"))
        self.teams.insert_many(self.game_data.to_dict("records"))


class MongoConnectionSeason:
    client: pymongo.MongoClient

    def __init__(self, obj: SeasonData):
        self.client = pymongo.MongoClient("mongodb://root:password@mongo:27017/")
        self.db = self.client["euroleague_dashboard"]

        self.season_data = obj

        self.lineups = self.db["lineups"]
        self.players = self.db["players"]
        self.teams = self.db["teams"]

        self.lineups_agg = self.db["lineups_agg"]
        self.players_agg = self.db["players_agg"]
        self.teams_agg = self.db["teams_agg"]

    def insert_season(self):
        self.lineups.insert_many(self.season_data.lineup_data.to_dict("records"))
        self.players.insert_many(self.season_data.player_data.to_dict("records"))
        self.teams.insert_many(self.season_data.team_data.to_dict("records"))

        self.lineups_agg.insert_many(self.season_data.lineup_data_agg.to_dict("records"))
        self.players_agg.insert_many(self.season_data.player_data_agg.to_dict("records"))
        self.teams_agg.insert_many(self.season_data.team_data_agg.to_dict("records"))
