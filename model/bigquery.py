import io
from google.cloud import bigquery


class BQLoader:
	def __init__(self, dataset_name, is_appengine, client_secrets_path=None):
		self.bq = bigquery.Client() if is_appengine else bigquery.Client.from_service_account_json(client_secrets_path)
		self.ds = self.bq.dataset(dataset_name)
	def loadCSV(self, table_name, data):
		table_ref = self.ds.tablr(table_name)
		job_config = bigquery.LoadJobConfig()
		job_config.autodetect = True
		job_config.source_format = bigquery.SourceFormat.CSV
		job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
		job = self.bq.load_table_from_file(
			io.BytesIO(data).encode('utf-8'),
			table_ref,
			location='EU',
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
		return 'successfull upload job for {0} rows'.format(job.output_rows)
