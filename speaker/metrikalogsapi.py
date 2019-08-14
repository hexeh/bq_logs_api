import time
from datetime import timedelta
from model import YandexAPIClient


class MetrikaLogsApi(YandexAPIClient):

	def __init__(self, app_config, credentials, polling_options, fields_options):
		super(MetrikaLogsApi, self).__init__(app_config, credentials)
		self.polling_options = polling_options
		self.field_options = fields_options

	def touchRequest(self, counter_id, request_params):
		"""
		Make evaluation on report creation possibility by passed parameters
		"""
		return self.requestProto(
			'GET', 'api-metrika', 'management/v1/counter/{}/logrequests/evaluate'.format(counter_id),
			params={
			**request_params,
				'date1': request_params['date1'].strftime('%Y-%m-%d'),
				'date2': request_params['date2'].strftime('%Y-%m-%d'),
			},
			restricted=True
		).get('log_request_evaluation')

	def composeRequestsChain(self, counter_id, request_params):
		requests_chain = []
		estimation = self.touchRequest(counter_id, request_params)
		if estimation['possible']:
			requests_chain.append({**request_params, 'status': 'new'})
		elif estimation['max_possible_day_quantity'] != 0:
			prolongation = (request_params['end_data'] - request_params['start_date']).days
			requests_count = int(prolongation/estimation['max_possible_day_quantity']) + 1
			request_interval = int(prolongation/requests_count) + 1
			for i in range(request_interval):
				requests_chain.append({
					**request_params,
					'status': 'new',
					'date1': request_params['date1'] + timedelta(i * request_interval),
					'date2': min(
						request_params['date2'],
						request_params['date1'] + timedelta( (i + 1) * request_interval - 1)
					)
				})
		else:
			raise RuntimeError('Logs API can\'t load data: max_possible_day_quantity = 0')
		return requests_chain

	def listRequests(self, counter_id):
		"""
		Get list of requested reports for specified counter
		"""
		return self.requestProto(
			'GET', 'api-metrika', 'management/v1/counter/{}/logrequests'.format(counter_id),
			restricted=True
		).get('requests')

	def createRequest(self, counter_id, request_params):
		"""
		Create new request for given counter and params
		"""
		return self.requestProto(
			'POST', 'api-metrika', 'management/v1/counter/{}/logrequests'.format(counter_id),
			params={
				**request_params,
				'date1': request_params['date1'].strftime('%Y-%m-%d'),
				'date2': request_params['date2'].strftime('%Y-%m-%d'),
			},
			restricted=True
		).get('log_request')

	def cancelRequest(self, counter_id, request_id):
		return self.requestProto(
			'POST', 'api-metrika', 'management/v1/counter/{}/logrequest/{}/cancel'.format(counter_id, request_id),
			restricted=True
		).get('log_request')

	def cleanRequest(self, counter_id, request_id):
		return self.requestProto(
			'POST', 'api-metrika', 'management/v1/counter/{}/logrequest/{}/clean'.format(counter_id, request_id),
			restricted=True
		).get('log_request')

	def downloadRequestPart(self, counter_id, request_id, part_no):
		raw_content = self.requestProto(
			'POST', 'api-metrika',
			'management/v1/counter/{}/logrequest/{}/part/{}/download'.format(counter_id, request_id, part_no)
		)
		lines_content = raw_content.split('\n')
		headers_num = len(lines_content[0].split('\t'))
		lines_filtered = list(filter(lambda x: len(x.split('\t')) == headers_num, raw_content.split('\n')))
		num_filtered = len(lines_content) - len(lines_filtered)
		if len(num_filtered) != 0:
			self.logger.warning('{} lines were filtered'.format(num_filtered))
		if len(lines_filtered) > 1:
			output_data = '\n'.join(lines_filtered)
			return output_data
		else:
			self.logger.warning('no content')
			return ''

	def describeRequest(self, counter_id, request_id):
		"""
		Get request description
		"""
		return self.requestProto(
			'GET', 'api-metrika', 'management/v1/counter/{}/logrequest/{}'.format(counter_id, request_id),
			restricted=True
		).get('log_request')

	def processRequest(self, counter_id, params: dict):
		if 'fields' not in params.keys():
			params['fields'] = self.field_options[params['source']]
		params['fields'] = ','.join(params['fields'])
		for poll in range(max(1, self.polling_options['retries'])):
			time.sleep((poll - 1) * self.polling_options['retries_delay'])
			chain = self.composeRequestsChain(counter_id, params)
			chain_results = []
			for request in chain:
				request = self.createRequest(chain)
				while request['status'] != 'processed':
					time.sleep(self.polling_options['retries_delay'])
					request = self.describeRequest(counter_id, ['request_id'])
				request_out = ''
				for part in range(request['size']):
					part_out = self.downloadRequestPart(counter_id, request['request_id'], part).split('\n')
					request_out += '\n' + part_out[(1 if part > 0 else 0):]
				chain_results.append(request_out)
				self.cleanRequest(counter_id, request['request_id'])
		return chain_results