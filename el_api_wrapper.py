import json
import sys

import requests
from tqdm.auto import tqdm


class ELAPI:

    def __init__(self):
        self.root_url = 'https://live.euroleague.net/api/'

    # TODO points endpoint
    # https://live.euroleague.net/api/Points?gamecode=329&seasoncode=E2022&disp=

    def get_points(self, season, game):
        query_url = f"{self.root_url}Points"
        params = {"gamecode": str(game),
                  "seasoncode": f'E{season}'}

        try:
            req = requests.get(query_url, params=params)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            raise err_h
        except requests.exceptions.ConnectionError as err_c:
            raise err_c
        except requests.exceptions.Timeout as err_t:
            raise err_t

        try:
            return req.json()
        except json.JSONDecodeError as err:
            return -1

    # TODO players endpoint
    # https://live.euroleague.net/api/Players?gamecode=329&seasoncode=E2022&disp=&equipo=MCO&temp=E2022

    def get_players(self, season, game, team):
        query_url = f'{self.root_url}Players'
        params = {'gamecode': game,
                  'seasoncode': f'E{season}',
                  'temp': f'E{season}',
                  'equipo': team}

        try:
            req = requests.get(query_url, params=params)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            raise err_h
        except requests.exceptions.ConnectionError as err_c:
            raise err_c
        except requests.exceptions.Timeout as err_t:
            raise err_t

        try:
            return req.json()
        except json.JSONDecodeError as err:
            return -1

    # TODO header endpoint
    # https://live.euroleague.net/api/Header?gamecode=329&seasoncode=E2022&disp=

    def get_header(self, season, game):
        query_url = f'{self.root_url}Header'
        params = {"gamecode": game,
                  "seasoncode": f'E{season}'}

        try:
            req = requests.get(query_url, params=params)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            raise err_h
        except requests.exceptions.ConnectionError as err_c:
            raise err_c
        except requests.exceptions.Timeout as err_t:
            raise err_t

        try:
            return req.json()
        except json.JSONDecodeError as err:
            return -1

    # TODO PlayByPlay endpoint
    # https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2022&disp=

    def get_playbyplay(self, season, game):
        query_url = f'{self.root_url}PlayByPlay'
        params = {"gamecode": game,
                  "seasoncode": f'E{season}'}

        try:
            req = requests.get(query_url, params=params)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            raise err_h
        except requests.exceptions.ConnectionError as err_c:
            raise err_c
        except requests.exceptions.Timeout as err_t:
            raise err_t

        try:
            return req.json()
        except json.JSONDecodeError as err:
            return -1

    # TODO game_stats, handle errors

    def get_game_stats(self, season, game):
        header = self.get_header(season, game)

        if header == -1:
            raise json.JSONDecodeError('Non-Existent Game', f"{season}-{game}", 1)
        else:
            home_team = header.get("CodeTeamA")
            away_team = header.get("CodeTeamB")

            points = self.get_points(season, game)
            home_players = self.get_players(season, game, home_team)
            away_players = self.get_players(season, game, away_team)
            play_by_play = self.get_playbyplay(season, game)

            game_dict = {"season": season,
                         "game": game,
                         "homeTeam": home_team,
                         "awayTeam": away_team,
                         "points": points,
                         "homePlayers": home_players,
                         "awayPlayers": away_players,
                         "playByPlay": play_by_play}

            return game_dict

    def get_number_of_games(self, season):

        query_url = f"https://feeds.incrowdsports.com/provider/euroleague-feeds/v2/competitions/E/seasons/E{season}/games?"

        if season == 2019:
            params = {"phaseTypeCode":"RS"}
        else:
            params = {"phaseTypeCode": "FF"}

        try:
            req = requests.get(query_url, params=params)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            raise err_h
        except requests.exceptions.ConnectionError as err_c:
            raise err_c
        except requests.exceptions.Timeout as err_t:
            raise err_t

        try:
            req = req.json().get("data")
            codes_list = [x.get("code") for x in req]
            return max(codes_list)
        except json.JSONDecodeError as err:
            return -1

    def get_season_stats(self, season, game=1):
        json_error_count = 50
        game = game
        max_game_count = self.get_number_of_games(season)
        season_games_list = []

        pbar = tqdm(total=max_game_count)

        while (json_error_count > 0) & (game <= max_game_count):
            try:
                game_dict = self.get_game_stats(season, game)
                season_games_list.append(game_dict)
            except json.JSONDecodeError:
                json_error_count = json_error_count - 1
            except:
                e = sys.exc_info()[0]
                with open("dnm.json", "w") as file:
                    json.dump(season_games_list, file)

                print('Unhandled Exception: ', e, "game: ", game, " season: ", season, " .json file saved")

            game = game + 1
            pbar.update(1)
        pbar.close()
        return season_games_list
