import copy
import datetime
import json

from flask import Flask, request, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import os
from prometheus_client.core import CounterMetricFamily, HistogramMetricFamily, REGISTRY
from flask_apscheduler import APScheduler

app = Flask(__name__)

scheduler = APScheduler()
scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()

BUCKETS_KEY = "buckets"
SUM_KEY = "sum"
METRICS_PUSH_DATE = "push_date"
ENDPOINT_INFO = "endpoint_info"


def json_open():
    with open("valid_rests.json", encoding='utf-8', errors='ignore') as json_data:
        endpoints_list = json.loads(json_data.read().strip(), strict=False)
    return endpoints_list


class CustomServiceExporter:
    stored_latency = {}
    endpoint_req_count = {}
    BUCKETS = {
        "0.01": 0,
        "0.5": 0,
        "0.1": 0,
        "2": 0,
        "3": 0,
        "4": 0,
        "10": 0
    }

    def collect(self):
        request_total = CounterMetricFamily(
            "http_service_selfcheck_status",
            "Service HTTP requests count with 'ok' or 'fail' status code",
            labels=["rest", "selfcheck_status"]
        )

        latency = HistogramMetricFamily(
            "http_service_latency",
            "Service HTTP requests duration",
            labels=["endpoint"]
        )

        for endpoint, info in self.endpoint_req_count.items():
            for status, count in info["endpoint_info"].items():
                request_total.add_metric([endpoint, status], count)
        yield request_total

        for endpoint, storage in self.stored_latency.items():
            latency.add_metric([endpoint], list(storage[BUCKETS_KEY].items()), storage[SUM_KEY])
        yield latency


REGISTRY.register(CustomServiceExporter())


def selfcheck_tracking():
    data = request.json
    rest = data["rest"]
    status = data["status"]
    push_date = data["push_date"]
    endpoint_req_storage = CustomServiceExporter.endpoint_req_count.setdefault(rest, {})
    endpoint_info = endpoint_req_storage.setdefault(ENDPOINT_INFO, {})
    CustomServiceExporter.endpoint_req_count[rest][ENDPOINT_INFO][status] = endpoint_info.get(status, 0) + 1
    CustomServiceExporter.endpoint_req_count[rest][METRICS_PUSH_DATE] = push_date
    return Response(status=200)


def track_metrics():
    data = request.json
    endpoint = data["endpoint"]
    push_date = data["push_date"]
    duration = data["time"]
    endpoint_storage = CustomServiceExporter.stored_latency.setdefault(endpoint, {})
    bucket_storage = endpoint_storage.setdefault(BUCKETS_KEY, CustomServiceExporter.BUCKETS.copy())

    for buckets in bucket_storage.keys():
        if duration >= float(buckets):
            bucket_storage[buckets] += 1

    endpoint_storage[SUM_KEY] = endpoint_storage.get(SUM_KEY, 0) + duration
    endpoint_storage[METRICS_PUSH_DATE] = push_date
    return Response(status=200)


def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@scheduler.task('interval', id='do_cleaning', minutes=json_open()["purge_interval"], misfire_grace_time=900)
def cleaner():
    # print("I'm running")
    delta = datetime.timedelta(minutes=json_open()["purge_timeout"])
    del_list = []
    for endpoint, info in CustomServiceExporter.endpoint_req_count.items():
        metrics_push_date = datetime.datetime.strptime(info[METRICS_PUSH_DATE], "%Y-%m-%d %H:%M:%S.%f")
        if metrics_push_date <= datetime.datetime.now() - delta:
            del_list.append(endpoint)

    for endpoint, info in CustomServiceExporter.stored_latency.items():
        metrics_push_date = datetime.datetime.strptime(info[METRICS_PUSH_DATE], "%Y-%m-%d %H:%M:%S.%f")
        if metrics_push_date <= datetime.datetime.now() - delta:
            del_list.append(endpoint)

    temp_counter_dict = copy.deepcopy(CustomServiceExporter.endpoint_req_count)
    temp_histogram_dict = copy.deepcopy(CustomServiceExporter.stored_latency)
    for endpoint in del_list:
        if endpoint in temp_counter_dict.keys():
            # print(f"{endpoint} deleted from counter dict")
            del temp_counter_dict[endpoint]
        elif endpoint in temp_histogram_dict.keys():
            # print(f"{endpoint} deleted from histo dict")
            del temp_histogram_dict[endpoint]

    CustomServiceExporter.endpoint_req_count = copy.deepcopy(temp_counter_dict)
    CustomServiceExporter.stored_latency = copy.deepcopy(temp_histogram_dict)
    return Response(status=200)


app.add_url_rule("/metric-receiver", view_func=track_metrics, methods=["POST"])
app.add_url_rule("/selfcheck-receiver", view_func=selfcheck_tracking, methods=["POST"])
app.add_url_rule("/cleaner", view_func=cleaner, methods=["GET"])
app.add_url_rule("/metrics", view_func=metrics, methods=["GET"])


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port, use_reloader=False)
