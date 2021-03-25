
OBSELT
=====

Tools for Observable ELT

Why
-----
Use the best tool for the job.
For ELT, SQL is the best tool do the job.

- Process data where it lives.
- Expressive, we can take use of powerful vendor features (e.g. ARRAY) that we do not get with ORMs.

For background on observability and some design choices in ELT, see `here
<https://towardsdatascience.com/introducing-observable-self-documenting-elt-41aa8b124098/>`_.

This library is written in a way that you can pick which part you want and ignore the rest.
It is currently for the GCP stack but can be easily adapted to others.

Usage
-----
Keep all sql scripts in one directory(`VIREVOL_SQL_DIR`).

The sql files should have SQLs delimited by `;`.

Each SQL should be headed with what it is for.

E.g.

```
--- @ddl('table1 -> None')
select t1.*, @tid
from t1
where t1.id = @param1
;

--- @ddl('table1 -> table3')
merge into table3 t
using (
	select table1.*, @tid as tid
	from table1
	where table1.attr = @param2
) as s
on t.id = s.id
when matched then
update set
	t.value = s.value,
	t.last_tid = s.last_tid
when not matched then
insert row;
```

Style notes
------
- All SQL in lowercase. Less shouty.
- Use black-but-with-tabs-instead-of-spaces because I'm blind.
