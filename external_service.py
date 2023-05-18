import argparse
import threading
import time
import json
import datetime

import requests

session = requests.Session()
session.verify = False

# from requests.packages.urllib3.exceptions import InsecureRequestWarning
#
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

main_list = []
task_list = []
status_list = []


def parse_args():
    """ Парсинг параметров """
    parser = argparse.ArgumentParser(
        description='Arguments parsing function')
    parser.add_argument(
        '-t', '--tasks',
        help='Number of pushing metrics threads. 4 is default',
        default=4)
    parser.add_argument(
        '-d', '--delay',
        help='Pause before script restart. 60 seconds is default',
        default=60)
    args = parser.parse_args()
    return vars(args)


def get_config():
    """ Получение параметров """
    config = parse_args()
    return config


def json_open():
    with open("valid_rests.json", encoding='utf-8', errors='ignore') as json_data:
        endpoints_list = json.loads(json_data.read().strip(), strict=False)
    return endpoints_list


def status_checker():
    statuses_n_port = {}
    for rest, status in status_list:
        statuses_n_port.setdefault(rest, []).append(status)

    for rest, status_lst in statuses_n_port.items():
        # print(rest, status_lst)
        for status in status_lst:
            if status != 200:
                selfcheck_pushing("fail", rest)
                break
        else:
            selfcheck_pushing("OK", rest)
    status_list.clear()


def histo_metric_getter(*args):
    endpoint_data = args[0]
    endpoint = endpoint_data[0]
    access_token = endpoint_data[1]
    if access_token == "" or None:
        try:
            response = requests.get(url=endpoint)
        except Exception:
            # print(f"Ошибка при запросе: {e}")
            if endpoint in task_list:
                task_list.remove(endpoint)
            return
    else:
        try:
            response = requests.get(url=endpoint, headers={"Authorization": access_token})
        except Exception as e:
            # print(f"Ошибка при запросе: {e}")
            if endpoint in task_list:
                task_list.remove(endpoint)
            return
    status = response.status_code
    elapsed_time = response.elapsed.total_seconds()
    # print(f"endpoint {endpoint} pushing")
    payload = {"push_date": str(datetime.datetime.now()), "time": elapsed_time, "endpoint": endpoint}
    try:
        requests.post("http://localhost:5000/metric-receiver", json=payload)
    except Exception:
        # print(f"Ошибка при отправке полученных данных: {e}")
        if endpoint in task_list:
            task_list.remove(endpoint)
        return
    # print(f"endpoint '{endpoint}' pushing finished")
    if endpoint_data in task_list:
        task_list.remove(endpoint_data)
    return status


def status_getter(endpoint, access_token):
    rest = endpoint.split("/")[3]
    if access_token == "" or None:
        try:
            response = requests.get(url=endpoint)
        except Exception:
            # print(f"Ошибка при запросе: {e}")
            if endpoint in task_list:
                task_list.remove(endpoint)
            return
    else:
        try:
            response = requests.get(url=endpoint, headers={"Authorization": access_token})
        except Exception:
            # print(f"Ошибка при запросе: {e}")
            if endpoint in task_list:
                task_list.remove(endpoint)
            return
    status = response.status_code
    status_list.append((rest, status))
    # print(f"url {endpoint} checked")
    if endpoint in task_list:
        task_list.remove(endpoint)
    return status


def selfcheck_pushing(status, rest):
    payload = {"push_date": str(datetime.datetime.now()), "status": status, "rest": rest}
    try:
        requests.post("http://localhost:5000/selfcheck-receiver", json=payload)
    except Exception:
        return
    return status


def data_preparing(endpoints_list):
    endpoints = endpoints_list["hosts"]
    histo_list = []
    for h_metric in endpoints:
        try:
            histo_list.append((h_metric["host"] + h_metric["rest"] + h_metric["r_time"], h_metric["token"]))
        except KeyError:
            histo_list.append((h_metric["host"] + h_metric["rest"] + h_metric["r_time"], ""))
    histo_list.insert(0, "histo")
    main_list.append(histo_list)

    for counter_metric in endpoints_list["hosts"]:
        host = counter_metric["host"]
        rest = counter_metric["rest"]
        try:
            token = counter_metric["token"]
        except KeyError:
            token = ""
        counter_list = [host + rest + url for url in counter_metric["selfcheck"]]
        counter_list.insert(0, "counter")
        counter_list.insert(1, token)
        main_list.append(counter_list)

    threader(main_list)


def threader(urls_list):
    for lst in urls_list:
        if lst[0] == "histo":
            target = histo_metric_getter
            token = None
            endpoints_list = lst[1:]
        elif lst[0] == "counter":
            target = status_getter
            token = lst[1]
            endpoints_list = lst[2:]
        num_tasks = int(get_config()['tasks'])

        i = 0
        while True:
            if i == len(endpoints_list):
                break
            if endpoints_list[i] in task_list:
                # print(f"{endpoints_list[i][0]} до сих пор работает")
                i += 1
                continue
            elif len(task_list) == num_tasks:
                while True:
                    if len(task_list) < num_tasks:
                        break

                x = threading.Thread(target=target, args=(endpoints_list[i], token))
                x.start()
                task_list.append(endpoints_list[i])
            elif len(task_list) < num_tasks:
                x = threading.Thread(target=target, args=(endpoints_list[i], token))
                x.start()
                task_list.append(endpoints_list[i])
            i += 1


if __name__ == '__main__':
    while True:
        main_list.clear()
        status_checker()
        endpoints_list = json_open()
        data_preparing(endpoints_list)
        time.sleep(int(get_config()['delay']))
