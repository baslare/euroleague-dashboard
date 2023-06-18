import json
from el_api_wrapper import ELAPI

if __name__ == '__main__':
    el = ELAPI()
    response = el.get_season_stats(2022)

    with open("dnm.json","w") as file:
        json.dump(response, file)




