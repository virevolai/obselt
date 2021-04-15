import logging
from os import getenv
from random import choices
from string import ascii_letters, digits
from time import sleep
from typing import Dict

from google.cloud import bigquery

from .sql import create_sql_dict

__all__ = ["load_file", "get_tid", "run_elt"]

log = logging.getLogger("obselt.bq")

sql_dict, atelier_service_tracker = None, None
SCHEMA_NAME = "virevol"


def run_elt(bq_cl, service: str, step: str, params=None, sync_seconds: int = 5) -> str:
	global sql_dict
	if not sql_dict:
		sql_dir = getenv("VIREVOL_SQL_DIR")
		log.info(f"Loading sql files from {sql_dir}")
		sql_dict = create_sql_dict(sql_dir)

	return run_elt_from_dict(bq_cl, sql_dict, service, step, params, sync_seconds)


# TODO: thresh_error
def run_elt_from_dict(bq_cl, sql_dict, service: str, step: str, params=None, sync_seconds: int = 5) -> str:
	"""Runs the ELT job

	Args:
		bq_cl: Bigquery client
		service: Name of the service running this step
		step: Name of the step to run
		params (list(dict), optional): A dictionary of parameters (name, type, value)
		sync_seconds (int, optional): If specified, wait till job finishes, loop till this is true
	"""
	if not step in sql_dict:
		raise ValueError(f"Could not find step: {step}. Valid steps are {sql_dict.keys()}")

	log.info(f"Running step: {step}")
	sql = sql_dict.get(step)

	all_params = set(sql.params)
	if 'tid' in all_params:
		all_params.remove('tid')
	if not params and all_params:
		raise ValueError(f"step {step} requires params {all_params}. Please provide values.")

	if not params:
		params = []

	if 'tid' in sql.params:
		params.append({"name": "tid", "type": "STRING", "value": get_tid(service, "default")})

	job_config = bigquery.QueryJobConfig(
		# Run at batch priority, which won't count toward concurrent rate limit.
		priority=bigquery.QueryPriority.BATCH
	)
	job_config.query_parameters = [bigquery.ScalarQueryParameter(p["name"], p["type"], p["value"]) for p in params]

	elt_job = bq_cl.query(str(sql), location="US", job_config=job_config)

	while elt_job.state != "DONE" and sync_seconds:
		elt_job = bq_cl.get_job(elt_job.job_id, location=elt_job.location)
		log.info(
			"Job {} is currently in state {}. Will recheck in {} seconds".format(elt_job.job_id, elt_job.state, sync_seconds)
		)
		sleep(sync_seconds)

	return elt_job.job_id


def __connect_bq():
	global bq_cl
	if not bq_cl:
		bq_cl = bigquery.Client()
	return bq_cl


def get_ddl():
	"""Gets DDL configuration for the table"""
	return {
		# BigQuery atelier project table for now
		# This table keeps track of all trace ids for originating requests
		# TODO: This should be partitioned by time
		"service_tracker": {
			"added_ts": {"field_type": "TIMESTAMP", "mode": "REQUIRED", "description": None},
			"service": {"field_type": "STRING", "mode": "REQUIRED", "description": "Name of the service"},
			"instance": {"field_type": "STRING", "mode": "REQUIRED", "description": "Name of the instance"},
			"tid": {"field_type": "STRING", "mode": "REQUIRED", "description": "Trace Id"},
		}
	}


def connect_bq_tbl(bq_cl, dataset_id, table_id):
	"""Create or connect to table_id"""
	dataset_ref = bq_cl.dataset(dataset_id)
	try:
		bq_cl.get_dataset(dataset_ref)
	except:
		log.info(f"Creating dataset: {dataset_id}")
		bq_cl.create_dataset(dataset_ref)

	table_ref = dataset_ref.table(table_id)
	try:
		table = bq_cl.get_table(table_ref)
	except:
		log.info(f"Creating table : {table_id}")

		ddl = get_ddl()

		if table_id not in ddl:
			raise "Table definition not found"
		schema = [bigquery.schema.SchemaField(**{"name": k, "fields": (), **c}) for k, c in ddl[table_id].items()]
		try:
			table = bq_cl.create_table(table=bigquery.Table(table_ref, schema))
			log.info(f"table created {table.schema}")
		except:
			log.info(f"Exception occured trying to create table")
			raise
	return table


def __connect_bq_tbl(dataset_id, table_id):
	"""Create or connect to table_id"""
	bq_cl = __connect_bq()
	return connect_bq_tbl(bq_cl, dataset_id, table_id)


def __connect_atelier_service_tracker():
	global atelier_service_tracker
	atelier_service_tracker = __connect_bq_tbl(SCHEMA_NAME, "service_tracker")


def service_trace_id(service: str, instance: str = "default", version: str = None, metadata: str = None):
	global atelier_service_tracker
	global bq_cl
	if not atelier_service_tracker or not bq_cl:
		__connect_atelier_service_tracker()

	tid = get_tid(service, instance)

	rows = [
		{
			"service": service.upper(),
			"instance": instance,
			"tid": tid,
			"version": version,
			"metadata": metadata,
			"added_ts": datetime.now(),
		}
	]

	errors = bq_cl.insert_rows(atelier_service_tracker, rows)

	if errors:
		log.error("Errors:")
		for error in errors:
			log.error(error)
	return json.dumps({"tid": tid})


def get_tid(service: str, instance: str) -> str:
	"""Returns a reasonably unique and short trace id
		For more on this, see
		https://towardsdatascience.com/introducing-observable-self-documenting-elt-41aa8b124098
	"""
	# 26 * 2 + 10 = 62 choose 8 choices
	tid = "".join(choices(ascii_letters + digits, k=8))
	return service[0].upper() + instance[0].upper() + tid


def load_file(bq_cl, tablename: str, src: Dict[str, str], dataset: str, thresh_error: int = 10000):
	"""Load data from Google Data Storage to Google BigQuery"""

	uri = f'gs://{src["bucket"]}/{src["folder"]}/{src["file_name"]}'

	dataset_ref = bq_cl.dataset(dataset)
	table_ref = dataset_ref.table(tablename)

	job_config = bigquery.LoadJobConfig()
	job_config.autodetect = True  # Auto detect schema
	job_config.max_bad_records = thresh_error
	job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

	load_job = bq_cl.load_table_from_uri(uri, table_ref, job_config=job_config)
	log.info(f"Starting job {load_job.job_id} for {tablename}")

	load_job.result()  # Waits for table load to complete

	destination_table = bq_cl.get_table(dataset_ref.table(tablename))
	log.info(f"Finished job {load_job.job_id} for {tablename}. Loaded {destination_table.num_rows} rows.")

	return load_job
