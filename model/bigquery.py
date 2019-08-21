import io
import gzip
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

	def checkTableExists(self, table_name):
		try:
			self.getTableInstance(table_name)
			return True
		except NotFound:
			return False

	def getTableInstance(self, table_name):
		return self.bq.get_table(self.ds.table(table_name))

	def getTableColumnMaxValue(self, table, column):
		if not self.checkTableExists(table):
			return []
		query = 'SELECT MAX({0!s}) as {0!s} FROM `{1!s}.{2!s}`'.format(
			column, self.ds.dataset_id, table
		)
		query_job = self.bq.query(query)
		job_result = query_job.result()
		return [row for row in job_result]

	def deleteFromTableOpenCondition(self, table, column, condition):
		query = 'DELETE FROM `{}.{}` WHERE {} {}'.format(
			self.ds.dataset_id, table, column, condition
		)
		query_job = self.bq.query(query)
		job_result = query_job.result()
		return [row for row in job_result]

	def loadCSV(self, table_name, data, options, clean=False, compress=True):
		table_ref = self.ds.table(table_name)
		if clean:
			if self.checkTableExists(table_name):
				table_i = self.getTableInstance(table_name)
				date_fields = [f.name for f in table_i.schema if f.field_type in ['DATE', 'DATETIME']]
				condition = 'BETWEEN {0!r} and {1!r}'.format(
					options['date1'].strftime('%Y-%m-%d') if 'request_id' not in options.keys() else options['date1'].split(' ')[0],
					options['date2'].strftime('%Y-%m-%d') if 'request_id' not in options.keys() else options['date2'].split(' ')[0]
				)
				self.deleteFromTableOpenCondition(table_name, date_fields[0], condition)
		job_config = bigquery.LoadJobConfig()
		job_config.autodetect = True
		job_config.source_format = bigquery.SourceFormat.CSV
		job_config.field_delimiter = '\t'
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
		job = self.bq.load_table_from_file(
			io.BytesIO(gzip.compress(data.encode('utf-8')) if compress else data.encode('utf-8')),
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
