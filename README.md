# bq_logs_api

Short remake of [logs_api_integration](https://github.com/yndx-metrika/logs_api_integration) with BigQuery upload.

This is not working version, please refer to [ToDo](todo.md)
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
```