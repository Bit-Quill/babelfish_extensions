/*-------------------------------------------------------------------------
 *
 * pltsql_identity.c		- Identity for PL/tsql
 *
 * IDENTIFICATION
 *	  contrib/pgtsql/src/pltsql_identity.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "parser/parser.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "multidb.h"
#include "pltsql.h"
#include "pltsql-2.h"
#include "session.h"

typedef struct SeqTableIdentityData
{
	Oid			relid;			/* pg_class OID of this sequence (hash key) */
	bool		last_identity_valid; /* check value validity */
	int64		last_identity;	/* sequence identity value */
} SeqTableIdentityData;

/*
 * By default, it is set to false.  This is set to true only when we want setval
 * to set the max/min(current identity value, new identity value to be inserted.
 */
bool pltsql_setval_identity_mode = false;

static HTAB *seqhashtabidentity = NULL;

static SeqTableIdentityData *last_used_seq_identity = NULL;

static Oid get_table_identity(Oid tableOid);

PG_FUNCTION_INFO_V1(get_identity_param);

/*
 * Given a table name with an identity column and a sequence option name,
 * fetch the identity sequence parameter value. Return NULL on error.
 */
Datum
get_identity_param(PG_FUNCTION_ARGS)
{
	text		*tablename = PG_GETARG_TEXT_PP(0);
	text		*optionname = PG_GETARG_TEXT_PP(1);
	int			 prev_sql_dialect = sql_dialect;

	sql_dialect = SQL_DIALECT_TSQL;
 
	PG_TRY();
	{
		RangeVar 	*tablerv;
		Oid 		tableOid;
		Oid			seqid;
		List		*seq_options;
		ListCell	*seq_lc;
		char		*cur_db_name;
		const char	*table = text_to_cstring(tablename);
		const char	*option = text_to_cstring(optionname);

		tablerv = pltsqlMakeRangeVarFromName(table);
		cur_db_name = get_cur_db_name();

		if (tablerv->schemaname && cur_db_name)
			tablerv->schemaname = get_physical_schema_name(cur_db_name,
														   tablerv->schemaname);

		/* Look up table name. Can't lock it - we might not have privileges. */
		tableOid = RangeVarGetRelid(tablerv, NoLock, false);

		/* Check permissions */
		if (pg_class_aclcheck(tableOid, GetUserId(), ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		{
			sql_dialect = prev_sql_dialect;
			PG_RETURN_NULL();
		}

		seqid = get_table_identity(tableOid);
		seq_options = sequence_options(seqid);

		foreach (seq_lc, seq_options)
		{
			DefElem *defel = (DefElem *) lfirst(seq_lc);

			if (strcmp(defel->defname, option) == 0)
			{
				sql_dialect = prev_sql_dialect;
				PG_RETURN_INT64(defGetInt64(defel));
			}
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	sql_dialect = prev_sql_dialect;

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(get_identity_current);

/*
 * Given a table name with an identity column, fetch the last identity sequence
 * value stored in the pg_sequences catalog. If not set, return the seed value
 * instead. Return NULL on error.
 */
Datum
get_identity_current(PG_FUNCTION_ARGS)
{
	text 		*tablename = PG_GETARG_TEXT_PP(0);
	const char	*table = text_to_cstring(tablename);
	RangeVar 	*tablerv;
	Oid 		tableOid;
	Oid			seqid = InvalidOid;
	List		*seq_options;
	ListCell	*seq_lc;
	int			prev_sql_dialect = sql_dialect;
	char		*cur_db_name;

	sql_dialect = SQL_DIALECT_TSQL;

	PG_TRY();
	{
		tablerv = pltsqlMakeRangeVarFromName(table);
		cur_db_name = get_cur_db_name();

		if (tablerv->schemaname && cur_db_name)
			tablerv->schemaname = get_physical_schema_name(cur_db_name,
														   tablerv->schemaname);

		/* Look up table name. Can't lock it - we might not have privileges. */
		tableOid = RangeVarGetRelid(tablerv, NoLock, false);

		/* Check permissions */
		if (pg_class_aclcheck(tableOid, GetUserId(), ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		{
			sql_dialect = prev_sql_dialect;
			PG_RETURN_NULL();
		}

		seqid = get_table_identity(tableOid);

		PG_TRY();
		{
			/* Check the tuple directly. Catch error if NULL */
			sql_dialect = prev_sql_dialect;
			return DirectFunctionCall1(pg_sequence_last_value,
									   ObjectIdGetDatum(seqid));
		}
		PG_CATCH();
		{
			FlushErrorState();
			sql_dialect = SQL_DIALECT_TSQL;
		}
		PG_END_TRY();

		/* If the relation exists, return the seed */
		if (seqid != InvalidOid)
		{
			seq_options = sequence_options(seqid);

			foreach (seq_lc, seq_options)
			{
				DefElem *defel = (DefElem *) lfirst(seq_lc);

				if (strcmp(defel->defname, "start") == 0)
				{
					sql_dialect = prev_sql_dialect;
					PG_RETURN_INT64(defGetInt64(defel));
				}
			}
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	sql_dialect = prev_sql_dialect;

	PG_RETURN_NULL();
}

/*
 * Get the table's identity sequence OID.
 */
static Oid
get_table_identity(Oid tableOid)
{
	Relation 	rel;
	TupleDesc 	tupdesc;
	AttrNumber	attnum;
	Oid			seqid = InvalidOid;

	rel = RelationIdGetRelation(tableOid);
	tupdesc = RelationGetDescr(rel);

	for (attnum = 0; attnum < tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);
		if (attr->attidentity)
		{
			seqid = getIdentitySequence(tableOid, attnum+1, false);
			break;
		}
	}

	RelationClose(rel);

	return seqid;
}

/*
 * Set the last identity value and update last_used_seq.
 */
void
pltsql_update_last_identity(Oid seqid, int64 val)
{
	SeqTableIdentityData	*elm;
	bool					found;

	if (seqhashtabidentity == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(SeqTableIdentityData);

		seqhashtabidentity = hash_create("Sequence values",
										 16,
										 &ctl,
										 HASH_ELEM | HASH_BLOBS);
	}

	/* Find or create an entry */
	elm = (SeqTableIdentityData *) hash_search(seqhashtabidentity,
											   &seqid,
											   HASH_ENTER,
											   &found);

	elm->last_identity_valid = true;
	elm->last_identity = val;

	last_used_seq_identity = elm;
}

int64
last_identity_value(void)
{
	/* Check if set and exists */
	if (last_used_seq_identity == NULL ||
		!SearchSysCacheExists1(RELOID,
							   ObjectIdGetDatum(last_used_seq_identity->relid)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("last identity not yet defined in this session")));

	if (!last_used_seq_identity->last_identity_valid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("last identity not valid")));

	if (pg_class_aclcheck(last_used_seq_identity->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence")));

	return last_used_seq_identity->last_identity;
}

void
pltsql_nextval_identity(Oid seqid, int64 val)
{
	if (prev_pltsql_nextval_hook)
		prev_pltsql_nextval_hook(seqid, val);

	if (sql_dialect == SQL_DIALECT_TSQL)
		pltsql_update_last_identity(seqid, val);
}

void pltsql_resetcache_identity()
{
	if (prev_pltsql_resetcache_hook)
		prev_pltsql_resetcache_hook();

	if (seqhashtabidentity)
	{
		hash_destroy(seqhashtabidentity);
		seqhashtabidentity = NULL;
	}

	last_used_seq_identity = NULL;
}

/*
 * BABELFISH: In T-SQL, with identity_insert=on, sequence value is set as
 * max(value to be inserted, last used sequence value) when increment is
 * positive. Min value is set with a negative increment.
 * We need to calculate the max/min and set the value without releasing
 * the lock so that other backends can't overwrite the value concurrently.
 */
int64
pltsql_setval_identity(Oid seqid, int64 val, int64 last_val)
{
	if (sql_dialect == SQL_DIALECT_TSQL && pltsql_setval_identity_mode)
	{
		ListCell *seq_lc;
		List *seq_options;
		int64 seq_incr = 0;

		seq_options = sequence_options(seqid);

		foreach (seq_lc, seq_options)
		{
			DefElem *defel = (DefElem *) lfirst(seq_lc);

			if (strcmp(defel->defname, "increment") == 0)
				seq_incr = defGetInt64(defel);

		}

		if (seq_incr > 0)
			val = val > last_val ? val : last_val;
		else
			val = val < last_val ? val : last_val;
	}

	return val;
}
