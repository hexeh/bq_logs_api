# -*- coding: utf-8 -*-
import requests
import sys
import re
import json
import logging
from datetime import datetime, timedelta


class YandexAPIClient:
	def __init__(self, app_config, credentials=None):
		self.app_config = app_config
		self.logger = logging.getLogger('api_yandex')
		logging.basicConfig(
			stream=sys.stdout,
			level=self.app_config.get('log_level', 'INFO'),
			format='%(asctime)s : %(processName)s : %(levelname)s : %(message)s',
			datefmt='%Y-%m-%d %H:%M:%S',
		)
		if credentials is not None:
			self.credentials = credentials
			safest_time = datetime.now() + timedelta(hours=10)
			if safest_time >= datetime.strptime(self.credentials['expired_at'], '%Y-%m-%d %H:%M:%S'):
				refresh_payload = {
					'grant_type': 'refresh_token',
					'refresh_token': self.credentials['refresh_token'],
					'client_id': self.app_config['client_id'],
					'client_secret': self.app_config['client_secret']
				}
				refresh_auth = self.requestProto('POST', 'oauth', 'token', refresh_payload)
				if refresh_auth is not None:
					refresh_auth['expired_at'] = (datetime.now() + timedelta(seconds=refresh_auth['expires_in']))\
						.strftime('%Y-%m-%d %H:%M:%S')
					self.credentials = refresh_auth
				else:
					self.logger.error('token refresh failed')
					raise Exception('Token refresh caused exception')
		else:
			self.logger.error('empty credentials')
			raise Exception('Please obtain new credentials via link: {0!s}'.format(self.returnOAuthLink()))

	def returnOAuthLink(self):
		return 'https://oauth.yandex.ru/authorize?response_type=code&client_id={}'.format(self.app_config['client_id'])

	def processOAuthCode(self, request):
		error_code = request.args.get('error', default=None)
		code = request.args.get('code', default=None)
		if error_code is None:
			exchange_payload = {
				'grant_type': 'authorization_code',
				'code': code,
				'client_id': self.app_config['client_id'],
				'client_secret': self.app_config['client_secret'],
			}
			exchanger = self.requestProto('POST', 'oauth', 'token', exchange_payload)
			token_response = json.loads(exchanger.text)
			if exchanger.status_code != 200:
				return False
			token_response['expired_at'] = (datetime.now() + timedelta(seconds=token_response['expires_in']))\
				.strftime('%Y-%m-%d %H:%M:%S')
			self.credentials = token_response
			return True

	def getCredentials(self):
		return self.credentials

	def getCountersList(self, params=None):
		raw_response = self.requestProto(
			'GET', 'api-metrika', 'management/v1/counters', params,
			restricted=True
		)
		if raw_response is not None:
			counters = raw_response['counters']
			if params is not None and int(params.get('per_page', 0) + params.get('offset', 1) - 1) < int(
					raw_response['rows']):
				params['offset'] = params['offset'] + len(counters)
				counters += self.getCountersList(params)
			else:
				return counters
		else:
			return None

	def getApplicationsList(self):
		raw_response = self.requestProto(
			'GET', 'api.appmetrica', 'management/v1/applications',
			restricted=True
		)
		if raw_response is not None:
			counters = raw_response['applications']
			return counters
		else:
			return None

	def getCounterGoals(self, counter_id, include_deleted=False):
		goals = self.requestProto(
			'GET', 'api-metrika', 'management/v1/counter/{}/goals'.format(counter_id),
			{'useDeleted': include_deleted},
			restricted=True
		)
		return goals

	def getCounterMeta(self, counter_id):
		meta = self.requestProto(
			'GET', 'api-metrika', 'management/v1/counter/{}'.format(counter_id),
			restricted=True
		)
		return meta

	def requestProto(self, http, service, point, params={}, restricted=False, content_type=None):
		if content_type is None:
			content_type = 'application/x-www-form-urlencoded'
		if restricted:
			auth_header = {
				'Authorization': 'OAuth {0!s}'.format(self.credentials['access_token'])
			}
		request_url = 'https://{0!s}.yandex.ru/{1!s}'.format(service, point)
		if http == 'POST':
			headers = {
				'Content-Type': content_type
			}
			if restricted:
				headers = dict(headers, **auth_header)
			request = requests.post(request_url, data=params, headers=headers)
		if http == 'GET':
			if restricted:
				request = requests.get(request_url, params=params, headers=auth_header)
			else:
				request = requests.get(request_url, params=params)
		request.encoding = 'utf-8'
		if request.status_code == 200:
			# even for tsv content-type is application/json
			# hello rfc: https://tools.ietf.org/html/rfc7231#section-3.1.1.5
			# if 'application/json' in request.headers.get('content-type'):
			# 	return json.loads(request.text)
			# return request.text
			try:
				return json.loads(request.text)
			except ValueError:
				return request.text
		else:
			self.logger.error('{} : {}'.format(point, request.text))
			return None
