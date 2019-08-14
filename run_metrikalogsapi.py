import json
from speaker import MetrikaLogsApi

if __name__ == '__main__':
	app_config_file = open('__test_configs/yandex_app_client.json')
	app_config = json.load(app_config_file)
	app_config_file.close()
	client_credentials_file = open('__test_configs/yandex_oauth_data.json')
	client_credentials = json.load(client_credentials_file)
	client_credentials_file.close()
	logs_api = MetrikaLogsApi(app_config, client_credentials, {
		'retries': 1,
		'retries_delay': 20
	}, {
		'visits': [
			"ym:s:counterID",
			"ym:s:dateTime",
			"ym:s:date",
			"ym:s:clientID"
		],
		'hits': [
			"ym:pv:counterID",
			"ym:pv:dateTime",
			"ym:pv:date",
			"ym:pv:clientID"
		]
	})
	print(logs_api.listRequests(46417356))