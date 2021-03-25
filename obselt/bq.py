import logging
from os import getenv
from random import choices
from string import ascii_letters, digits
from time import sleep

from google.cloud import bigquery

from sql import create_sql_dict

__all__ = ["load_file", "get_tid", "run_elt"]

log = logging.getLogger("obselt.bq")
sql_dict = None


# TODO: thresh_error
def run_elt(bq_cl, service: str, step: str, params=None, sync_seconds: int = 5) -> str:
	"""Runs the ELT job

	Args:
		bq_cl: Bigquery client
		service: Name of the service running this step
		step: Name of the step to run
		params (list(dict), optional): A dictionary of parameters (name, type, value)
		sync_seconds (int, optional): If specified, wait till job finishes, loop till this is true
	"""
	global sql_dict
	if not sql_dict:
		sql_dir = getenv("VIREVOL_SQL_DIR")
		log.info(f"Loading sql files from {sql_dir}")
		sql_dict = create_sql_dict(sql_dir)

	if not step in sql_dict:
		raise ValueError(f"Could not find step: {step}. Valid steps are {sql_dict.keys()}")

	log.info(f"Running step: {step}")
	sql = sql_dict.get(step)

	if not params and len(sql.params) > 1:  # We will always have a @tid param
		raise ValueError(f"step {step} requires params {sql.params}. Please provide values")

	if not params:
		params = []

	params.append({"name": "@tid", "type": "string", "value": get_tid(service_name, "default")})

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


def get_tid(service: str, instance: str) -> str:
	"""Returns a reasonably unique and short trace id
		For more on this, see
		https://towardsdatascience.com/introducing-observable-self-documenting-elt-41aa8b124098
	"""
	# 26 * 2 + 10 = 62 choose 8 choices
	tid = "".join(choices(ascii_letters + digits, k=8))
	return service[0].upper() + instance[0].upper() + tid


def load_file(bq_cl, tablename: str, src: Dict[str, str, str], dataset: str, thresh_error: int = 10000):
	"""Load data from Google Data Storage to Google BigQuery"""

	uri = f'gs://{src["bucket"]}/{src["folder"]}/{src["file_name"]}'

	dataset_ref = bq_cl.dataset(dataset)
	table_ref = dataset_ref.table(tablename)

	job_config = bigquery.LoadJobConfig()
	job_config.autodetect = True
	job_config.max_bad_records = thresh_error
	# TODO: Figure out how to partition by load time, for now, truncate on reload
	# job_config.time_partitioning = bigquery.TimePartitioning
	job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

	load_job = bq_cl.load_table_from_uri(uri, table_ref, job_config=job_config)
	log.info(f"Starting job {load_job.job_id} for {tablename}")

	load_job.result()  # Waits for table load to complete

	destination_table = bq_cl.get_table(dataset_ref.table(tablename))
	log.info(f"Finished job {load_job.job_id} for {tablename}. Loaded {destination_table.num_rows} rows.")

	return load_job
