import io
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BQLoader:
	def __init__(self, dataset_name, is_appengine, client_secrets_path=None, location='EU'):
		self.bq = bigquery.Client() if is_appengine else bigquery.Client.from_service_account_json(client_secrets_path)
		self.ds = self.bq.dataset(dataset_name)
		self.loc = location
		try:
			self.bq.get_dataset(self.ds)
		except NotFound:
			ds = bigquery.Dataset(self.ds)
			ds.location = self.loc
			self.bq.create_dataset(ds)

	def getTableColumnMaxValue(self, column):
		query = 'SELECT MAX({0!s}) as {0!s} FROM {1!s}.{2!s}'.format(
			column, self.ds
		)
		query_job = self.bq.query(query)
		job_result = query_job.result()
		return [row for row in job_result]

	def loadCSV(self, table_name, data):
		table_ref = self.ds.table(table_name)
		job_config = bigquery.LoadJobConfig()
		job_config.autodetect = True
		job_config.source_format = bigquery.SourceFormat.CSV
		job_config.field_delimiter = '\t'
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
		data = data.split('\n')
		data_header = data[0]
		for area in ['s', 'pv']:
			data_header.replace('ym_{}_'.format(area), '')
		data = '\n'.join(([data_header] + data[1:]))
		job = self.bq.load_table_from_file(
			io.BytesIO(data.encode('utf-8')),
			table_ref,
			location=self.loc,
			job_config=job_config
		)
		try:
			job.result()
		except Exception as e:
			if hasattr(job, 'errors') and job.errors is not None:
				if len(job.errors):
					raise RuntimeError(job.errors)
		finally:
			assert job.state == 'DONE'
		return 'successful upload job for {} rows'.format(job.output_rows)
