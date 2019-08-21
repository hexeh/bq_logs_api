import json
import argparse
import sys
from datetime import date, datetime, timedelta

from speaker import MetrikaLogsApi
from model import BQLoader

DATE_FORMATTER = '%Y-%m-%d'


def produceExportRange(options, i, db, counter_id, table_name):
	if options.mode == 'custom':
		date_range = [datetime.strptime(x, DATE_FORMATTER).date() for x in options.date_range]
	elif options.mode == 'regular':
		date_range = [date.today() - timedelta(days=2)]
	elif options.mode == 'regular_early':
		date_range = [date.today() - timedelta(days=1)]
	elif options.mode == 'history':
		counter_created = i.getCounterMeta(counter_id).get('counter')['create_time'].split('T')[0]
		date_range = [
			datetime.strptime(counter_created, DATE_FORMATTER).date(),
			date.today() - timedelta(days=2)]
	elif options.mode == 'detect':
		if db.checkTableExists(table_name):
			table_i = db.getTableInstance(table_name)
			date_fields = [f.name for f in table_i.schema if f.field_type in ['DATE', 'DATETIME']]
			persisted_date = db.getTableColumnMaxValue(table_name, date_fields[0])
			min_allowed_date = date.today() - timedelta(days=1)
			lower_date = persisted_date[0][0] + timedelta(days=1)
			if lower_date > min_allowed_date:
				i.logger.warning('up to date')
				sys.exit(0)
			date_range = [
				lower_date,
				max(lower_date, min_allowed_date)
			]
		else:
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
	parser = argparse.ArgumentParser(
		description='Command-line interface for Metrika Logs API',
		formatter_class=argparse.RawTextHelpFormatter
	)
	parser.add_argument(
		'-m', '--mode',
		choices=['history', 'regular', 'regular_early', 'custom', 'detect'],
		default='regular',
		help='Export mode: \n\t\033[1m history \033[0m - all counter data,'\
			'\n\t\033[1m regular \033[0m - data for past yesterday,'\
			'\n\t\033[1m regular_early \033[0m - data for yesterday,'\
			'\n\t\033[1m custom \033[0m - data for custom range (--date_range parameter need to be filled),'\
			'\n\t\033[1m detect \033[0m - detect table data and upload only new dates'
	)
	parser.add_argument(
		'-dr', '--date_range',
		nargs='*',
		default=(date.today() - timedelta(days=2)).strftime(DATE_FORMATTER),
		help='Date range of stats to export (format: \033[1m YYYY-MM-DD \033[0m), \nWorks with custom mode only'
	)
	parser.add_argument(
		'-t', '--type',
		choices=['hits', 'visits'],
		default='visits',
		help='Source table to export stats'
	)
	parser.add_argument(
		'-c', '--clean',
		action='store_true',
		help='Whether to clean data for same dates in database or not'
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
	table_name = 'counter_{}'.format(counter_id)
	# Check Execution info
	execution_params = produceExportRange(args, logs_api, bq, counter_id, table_name)
	# Check previous runs
	old_requests = logs_api.listRequests(counter_id)
	if len(old_requests) > 0:
		print('cleaning {} old requests'.format(len(old_requests)))
	for req in old_requests:
		if req['status'] == 'created':
			request_data = logs_api.pollRequest(counter_id, req, created=True)
			logs_api.logger.info(bq.loadCSV(table_name, request_data, options=req, clean=args.clean))
		if req['status'] == 'processed':
			request_data = logs_api.saveRequest(counter_id, req)
			logs_api.logger.info(bq.loadCSV(table_name, request_data, options=req, clean=args.clean))
			logs_api.logger.info(logs_api.cleanRequest(counter_id, req['request_id']))

	# Work with new run
	request_data = logs_api.processRequest(counter_id, execution_params)
	request_data = '\n'.join([file[(1 if idx > 0 else 0):] for idx, file in enumerate(request_data)])
	logs_api.logger.info(bq.loadCSV(table_name, request_data, options=execution_params, clean=args.clean))
