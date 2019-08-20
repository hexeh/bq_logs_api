import json
import argparse
import sys
from datetime import date, datetime, timedelta

from speaker import MetrikaLogsApi
from model import BQLoader

DATE_FORMATTER = '%Y-%m-%d'

def produceExportRange(options, i, counter_id):
	if options.mode == 'custom':
		date_range = [datetime.strptime(x, DATE_FORMATTER).date() for x in options.date_range]
	if options.mode == 'regular':
		date_range = [date.today() - timedelta(days=2)]
	if options.mode == 'regular_early':
		date_range = [date.today() - timedelta(days=1)]
	if options.mode == 'history':
		counter_created = i.getCounterMeta(counter_id).get('counter')['create_time'].split('T')[0]
		date_range = [
			datetime.strptime(counter_created, DATE_FORMATTER).date(),
			date.today() - timedelta(days=2)]
	if len(date_range) == 1:
		date_range.append(date_range[0])

	return {
		'source': options.type,
		'date1': date_range[0],
		'date2': date_range[1]
	}


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Command-line interface for Metrika Logs API client')
	parser.add_argument(
		'-m', '--mode',
		choices=['history', 'regular', 'regular_early', 'custom'],
		default='regular',
		help='Export mode'
	)
	parser.add_argument(
		'-dr', '--date_range',
		nargs='*',
		default=(date.today() - timedelta(days=2)).strftime(DATE_FORMATTER),
		help='Date range of stats to export (format: YYYY-MM-DD)'
	)
	parser.add_argument(
		'-t', '--type',
		choices=['hits', 'visits'],
		default='visits',
		help='Source table to export stats'
	)
	args = parser.parse_args()
	if len(args.date_range) != 1 and len(args.date_range) > 2 and args.mode == 'custom':
		print('--date-range parameter should include only 1 or 2 dates')
		sys.exit(-1)
	# Read Configuration Options
	app_config_file = open('__test_configs/yandex_app_client.json')
	app_config = json.load(app_config_file)
	app_config_file.close()
	client_credentials_file = open('__test_configs/yandex_oauth_data.json')
	client_credentials = json.load(client_credentials_file)
	client_credentials_file.close()
	run_config_file = open('__run_configs/configuration.json')
	run_config = json.load(run_config_file)
	run_config_file.close()
	counter_id = run_config['counter_id']
	# Create Instances
	logs_api = MetrikaLogsApi(app_config, client_credentials, run_config['poll'], run_config['fields'])
	bq = BQLoader(run_config['dataset'], False, '__test_configs/client_secrets.json')
	# Check Execution info
	execution_params = produceExportRange(args, logs_api, counter_id)
	# Check previous runs
	old_requests = logs_api.listRequests(counter_id)
	if len(old_requests) > 0:
		print('cleaning {} old requests'.format(len(old_requests)))
	for req in old_requests:
		if req['status'] == 'created':
			request_data = logs_api.pollRequest(counter_id, req, created=True)
			logs_api.logger.info(bq.loadCSV('counter_{}'.format(counter_id), request_data))
		if req['status'] == 'processed':
			request_data = logs_api.saveRequest(counter_id, req)
			bq.loadCSV('counter_{}'.format(counter_id), request_data)
			logs_api.logger.info(logs_api.cleanRequest(counter_id, req['request_id']))

	# Work with new run
	request_data = logs_api.processRequest(counter_id, execution_params)
	request_data = '\n'.join([file[(1 if idx > 0 else 0):] for idx, file in enumerate(request_data)])
	logs_api.logger.info(bq.loadCSV('counter_{}'.format(counter_id), request_data))
