from pathlib import Path
from unittest import TestCase

from obselt import sql


class SqlLint(TestCase):
	script1 = """
		---- @ddl('t1 -> t2')
		select t1.*
		from t1
		where t1.id0 = @my_param0
		and t1.id1 = @my_param1
		;
	"""

	script2 = """
		-- @ddl('t1 -> t3')
		select t1.*
		from t1
		where t1.id0 = @my_param0
		;
	"""

	def test_valid_script(self):
		script = """
			select 1;
			select 2;
		"""
		parsed = list(sql.parse_sql_script(script))
		assert len(parsed) == 2
		# First line of first SQL found
		assert parsed[0][0] == "select 1"

	def test_invalid_script(self):
		script = """
			select 1
			select 2
		"""
		parsed = list(sql.parse_sql_script(script))
		assert len(parsed) == 0

	def test_parse_param(self):
		sqls = list(sql.parse_sql_script(self.script1))
		command, params = sql.get_command(sqls[0])

		assert command == "t1 -> t2"
		assert params[0] == "@my_param0"
		assert params[1] == "@my_param1"

	def test_sql_dict(self):
		from string import ascii_letters, digits
		from random import choices

		def fake_name(ext=True):
			name = "".join(choices(ascii_letters + digits, k=8))
			return f"{name}.sql" if ext else name

		dir_name = fake_name(False)
		dir_name = Path(dir_name)
		fname1, fname2 = [fake_name() for _ in range(2)]

		dir_name.mkdir()
		with open(dir_name / fname1, "w") as f:
			f.write(self.script1)

		with open(dir_name / fname2, "w") as f:
			f.write(self.script2)

		sql_dict = sql.create_sql_dict(dir_name)

		# cleanup
		(dir_name / fname1).unlink()
		(dir_name / fname2).unlink()
		dir_name.rmdir()

		assert len(sql_dict) == 2
		assert "t1 -> t2" in sql_dict
		assert len(sql_dict["t1 -> t2"].params) == 2
