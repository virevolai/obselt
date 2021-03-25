import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional

_all_ = ["parse_sql_script", "get_command", "create_sql_dict", "SQL"]

DELIM = ";"
COMMENT = "--"
KEYWORD = "ddl"

# TODO: Write tests for this regex
CMD = rf"^--.*@{KEYWORD}\('(.*?)'\)"
PARAM = r"(@\w*)"
RE_CMD = re.compile(CMD)
RE_PARAM = re.compile(PARAM)


@dataclass(frozen=True)
class SQL:
	sql: List[str]
	params: Optional[List[str]]

	def __str__(self):
		return "\n".join(self.sql)


def create_sql_dict(dir: str) -> Dict[str, SQL]:
	"""Creates a dictionary of all SQL in a directory"""
	sql_dict = {}
	dir = Path(dir)

	if not dir.is_dir():
		raise ValueError("dir should be a directory with sql files in it")

	for fname in dir.glob("*.sql"):
		with open(fname, "r") as script:
			sqls = parse_sql_script(script.read())
			for sql in sqls:
				command, params = get_command(sql)
				sql_dict[command] = SQL(sql, params)

	return sql_dict


def parse_sql_script(sql_script: str) -> Iterator[Iterator[str]]:
	"""Parses sql script into individual sqls"""
	sql = []

	for line in sql_script.splitlines():

		if is_comment(line):
			pass

		if DELIM in line:

			sql.append(line.split(DELIM)[0])
			yield [s.strip() for s in sql if s.strip()]

			# Initialize sql
			# TODO: Handle multiple DELIM in same line
			sql = line.split(DELIM)[1:]

		elif len(line) > 0:
			sql.append(line)


def is_comment(sql: str) -> bool:
	return sql.strip()[:2] == COMMENT


def get_command(sql_lst: List[str]) -> [str, Optional[Iterator[str]]]:
	"""Get the command and params (if any)"""
	# By convention, the command is in the first line

	m = re.search(RE_CMD, sql_lst[0])

	if not m or not is_comment(sql_lst[0]):
		raise ValueError("No commands found. Expected in the first line")

	# Are there any params?
	params = get_params("\n".join(sql_lst[1:]))
	return m[1], params


def get_params(sql: str) -> Optional[Iterator[str]]:
	return re.findall(RE_PARAM, sql)
