from el_api_wrapper import ELAPI

if __name__ == '__main__':
    el = ELAPI()
    resp = el.get_points(season='2022', game='1')

    print(resp)

