# bq_logs_api

Short remake of [logs_api_integration](https://github.com/yndx-metrika/logs_api_integration) with BigQuery upload.

Mildly working prototype, for details please refer to [ToDo](todo.md)
## Configuration

```bash
$ cat __test_configs/yandex_app_client.json

{
	"app_name": "MyAppName",
	"client_id": "abcdef...sdfksdf",
	"client_secret": "shdfjsdbfjh",
	"callback_url": "oauth_callback",
	"log_level": "DEBUG"
}
```

```bash
$ cat __test_configs/yandex_oauth_data.json

{
	"access_token": "sdfsdfs",
	"expired_at": "2020-05-13 12:50:53",
	"expires_in": "14950245",
	"refresh_token": "1:fdsfds",
    "token_type": "bearer"
}
```

```bash
$ cat __run_configs/configuration.json

{
  "dataset": "MetrikaLogs",
  "counter_id": "758643",
  "poll": {
    "retries": 1,
    "retries_delay": 20
  },
  "fields": {
    "visits": [
      "ym:s:counterID",
      "ym:s:dateTime",
      "ym:s:date",
      "ym:s:clientID"
    ],
    "hits": [
      "ym:pv:counterID",
      "ym:pv:dateTime",
      "ym:pv:date",
      "ym:pv:clientID"
    ]
  }
}
```

## Run

```bash
python3 client.py --help
usage: client.py [-h] [-m {history,regular,regular_early,custom,detect}]
                 [-dr [DATE_RANGE [DATE_RANGE ...]]] [-t {hits,visits}] [-c]

Command-line interface for Metrika Logs API client

optional arguments:
  -h, --help            show this help message and exit
  -m {history,regular,regular_early,custusage: client.py [-h] [-m {history,regular,regular_early,custom,detect}]
                 [-dr [DATE_RANGE [DATE_RANGE ...]]] [-t {hits,visits}] [-c]

Command-line interface for Metrika Logs API

optional arguments:
  -h, --help            show this help message and exit
  -m {history,regular,regular_early,custom,detect}, --mode {history,regular,regular_early,custom,detect}
                        Export mode: 
                                 history  - all counter data,
                                 regular  - data for past yesterday,
                                 regular_early  - data for yesterday,
                                 custom  - data for custom range (--date_range parameter need to be filled),
                                 detect  - detect table data and upload only new dates
  -dr [DATE_RANGE [DATE_RANGE ...]], --date_range [DATE_RANGE [DATE_RANGE ...]]
                        Date range of stats to export (format:  YYYY-MM-DD ), 
                        Works with custom mode only
  -t {hits,visits}, --type {hits,visits}
                        Source table to export stats
  -c, --clean           Whether to clean data for same dates in database or not
om,detect}, --mode {history,regular,regular_early,custom,detect}
                        Export mode
  -dr [DATE_RANGE [DATE_RANGE ...]], --date_range [DATE_RANGE [DATE_RANGE ...]]
                        Date range of stats to export (format: YYYY-MM-DD)
  -t {hits,visits}, --type {hits,visits}
                        Source table to export stats
  -c, --clean           Whether to clean data for same dates in database or
                        not

```

### Run with automatic detection

```bash
 client.py --mode detect --type visits
```