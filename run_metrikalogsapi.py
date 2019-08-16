import json
from datetime import date, timedelta
from speaker import MetrikaLogsApi

if __name__ == '__main__':
	app_config_file = open('__test_configs/yandex_app_client.json')
	app_config = json.load(app_config_file)
	app_config_file.close()
	client_credentials_file = open('__test_configs/yandex_oauth_data.json')
	client_credentials = json.load(client_credentials_file)
	client_credentials_file.close()
	counter_id = client_credentials['counter_id']
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
	old_requests = logs_api.listRequests(counter_id)
	print('cleaning {} old requests'.format(len(old_requests)))
	for req in old_requests:
		if req['status'] == 'created':
			print('canceling request: {}'.format(req))
			logs_api.cancelRequest(counter_id, req['request_id'])
		if req['status'] == 'processed':
			print('cleaning request: {}'.format(req))
			logs_api.cleanRequest(counter_id, req['request_id'])
	fout = open('test_run_{}.csv'.format(counter_id), 'w')
	request_data = logs_api.processRequest(counter_id, {
		'date1': date.today() - timedelta(days=3),
		'date2': date.today() - timedelta(days=1),
		'source': 'visits'
	})
	fout.write(request_data)
	fout.close()
