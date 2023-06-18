import requests
import pandas as pd


class ELAPI:

    def __init__(self):
        self.root_url = 'https://live.euroleague.net/api/'

    # TODO points endpoint
    # https://live.euroleague.net/api/Points?gamecode=329&seasoncode=E2022&disp=

    def get_points(self, season, game):
        query_url = f"{self.root_url}Points"
        params = {"gamecode": game,
                  "seasoncode": f'E{season}'}

        req = requests.get(query_url, params=params)

        if req.status_code != 200:
            raise requests.exceptions.ConnectionError

        return pd.DataFrame.from_dict(req.json())

    # TODO players endpoint
    # https://live.euroleague.net/api/Players?gamecode=329&seasoncode=E2022&disp=&equipo=MCO&temp=E2022

    def get_players(self, season, game, team):
        query_url = f'{self.root_url}Players'
        params = {'gamecode': game,
                  'season': f'E{season}',
                  'temp': f'E{season}',
                  'equipo': team}

        req = requests.get(query_url, params=params)
        return req.json()

    # TODO header endpoint
    # https://live.euroleague.net/api/Header?gamecode=329&seasoncode=E2022&disp=

    def get_header(self, season, game):
        query_url = f'{self.root_url}Header'
        params = {"gamecode": game,
                  "seasoncode": f'E{season}'}

        req = requests.get(query_url, params=params)

        return req.json()

    # TODO PlayByPlay endpoint
    # https://live.euroleague.net/api/PlayByPlay?gamecode=1&seasoncode=E2022&disp=

    def get_playbyplay(self, season, game):
        query_url = f'{self.root_url}PlayByPlay'
        params = {"gamecode": game,
                  "seasoncode": f'E{season}'}

        req = requests.get(query_url, params=params)

        return req.json()

    # TODO Individual Player endpoint
    # https://www.euroleaguebasketball.net/euroleague/_next/data/H7MAVz56-mMbim2cpFPa8/players/sasha-vezenkov/003469.json
