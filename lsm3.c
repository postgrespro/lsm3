#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/relscan.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"

#include "lsm3.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(lsm3_handler);
PG_FUNCTION_INFO_V1(lsm3_btree_wrapper);
extern void	_PG_init(void);
extern void	_PG_fini(void);

extern void lsm3_merger_main(Datum arg);

/* Lsm3 dictionary (hashtable with control data for all indexes) */
static Lsm3DictEntry* Lsm3Entry;
static HTAB*          Lsm3Dict;
static LWLock*        Lsm3DictLock;

/* Lsm3 kooks */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;
static shmem_startup_hook_type  PreviousShmemStartupHook = NULL;

/* Lsm3 GUCs */
static int Lsm3MaxIndexes;
static int Lsm3TopIndexHeight;

/* Background worker termination flag */
static volatile bool Lsm3Cancel;

static void
lsm3_shmem_startup(void)
{
	HASHCTL info;

	if (PreviousShmemStartupHook)
	{
		PreviousShmemStartupHook();
    }
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(Lsm3DictEntry);
	Lsm3Dict = ShmemInitHash("lsm3 hash",
							 Lsm3MaxIndexes, Lsm3MaxIndexes,
							 &info,
							 HASH_ELEM | HASH_BLOBS);
	Lsm3DictLock = &(GetNamedLWLockTranche("lsm3"))->lock;
}

/* Initialize Lsm3 control data entry */
static void
lsm3_init_entry(Lsm3DictEntry* entry, Oid heap_oid)
{
	SpinLockInit(&entry->spinlock);
	entry->active_index = 0;
	entry->merger = NULL;
	entry->merge_in_progress = false;
	entry->start_merge = false;
	entry->n_merges = 0;
	entry->top[0] = entry->top[1] = InvalidOid;
	entry->access_count[0] = entry->access_count[1] = 0;
	entry->heap = heap_oid;
}

/* Get B-Tree height */
static int
lsm3_get_height(Relation index)
{
	Buffer	buffer = ReadBuffer(index, 0);
	Page	page = BufferGetPage(buffer);
	BTMetaPageData *metad = BTPageGetMeta(page);
	int		height = metad->btm_level;
	ReleaseBuffer(buffer);
	return height;
}

/* Get B-Tree height given index Oid */
static int
lsm3_get_height_by_oid(Oid relid)
{
	Relation index = index_open(relid, AccessShareLock);
	int height = lsm3_get_height(index);
	index_close(index, AccessShareLock);
	return height;
}

/* Lookup or create Lsm3 control data for this index */
static Lsm3DictEntry*
lsm3_get_entry(Relation index)
{
	Lsm3DictEntry* entry;
	bool found = true;
	LWLockAcquire(Lsm3DictLock, LW_SHARED);
	entry = (Lsm3DictEntry*)hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_FIND, &found);
	if (entry == NULL)
	{
		/* We need exclusive lock to create new entry */
		LWLockRelease(Lsm3DictLock);
		LWLockAcquire(Lsm3DictLock, LW_EXCLUSIVE);
		entry = (Lsm3DictEntry*)hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_ENTER, &found);
	}
	if (!found)
	{
		char* relname = RelationGetRelationName(index);
		for (int i = 0; i < 2; i++)
		{
			char* topidxname = psprintf("%s_top%d", relname, i);
			entry->top[i] = get_relname_relid(topidxname, RelationGetNamespace(index));
			if (entry->top[i] == InvalidOid)
			{
				elog(ERROR, "Lsm3: failed to lookup %s index", topidxname);
			}
		}
		lsm3_init_entry(entry, index->rd_index->indrelid);
		entry->active_index = lsm3_get_height_by_oid(entry->top[0]) >= lsm3_get_height_by_oid(entry->top[1]) ? 0 : 1;
	}
	LWLockRelease(Lsm3DictLock);
	return entry;
}

/* Launch merger bgworker */
static void
lsm3_launch_bgworker(Lsm3DictEntry* entry)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t bgw_pid;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "lsm3-merger-%d", entry->base);
	snprintf(worker.bgw_type, sizeof(worker.bgw_type), "lsm3-merger-%d", entry->base);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_function_name, "lsm3_merger_main");
	strcpy(worker.bgw_library_name, "postgres");
	worker.bgw_main_arg = PointerGetDatum(entry);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(ERROR, "Lsm3: failed to start background worker");
	}
	if (WaitForBackgroundWorkerStartup(handle, &bgw_pid) != BGWH_STARTED)
	{
		elog(ERROR, "Lsm3: startup of background worker is failed");
	}
	entry->merger = BackendPidGetProc(bgw_pid);
	if (entry->merger == NULL)
	{
		elog(ERROR, "Lsm3: background worker is crashed");
	}
}

/* Cancel merger bgwroker */
static void
lsm3_merge_cancel(int sig)
{
	Lsm3Cancel = true;
	SetLatch(MyLatch);
}

/* Truncate top index */
static void
lsm3_truncate_index(Oid index_oid, Oid heap_oid)
{
	Relation index = index_open(index_oid, AccessExclusiveLock);
	Relation heap = table_open(heap_oid, AccessShareLock);
	IndexInfo* indexInfo = BuildDummyIndexInfo(index);
	RelationTruncate(index, 0);
	index_build(heap, index, indexInfo, true, false);
	index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}

/* Merge top index into base index */
static void
lsm3_merge_indexes(Oid dst_oid, Oid src_oid, Oid heap_oid)
{
	Relation top_index = index_open(src_oid, AccessShareLock);
	Relation heap = table_open(heap_oid, AccessShareLock);
	Relation base_index = index_open(dst_oid, RowExclusiveLock);
	IndexScanDesc scan = btbeginscan(top_index, 0, 0);
	bool ok;
	scan->xs_want_itup = true;
	for (ok = _bt_first(scan, ForwardScanDirection); ok; ok = _bt_next(scan, ForwardScanDirection))
	{
		if (!_bt_doinsert(base_index, scan->xs_itup, false, heap))
		{
			elog(ERROR, "Lsm3: failed to insert tuple in base index");
		}
	}
	btendscan(scan);
	index_close(top_index, AccessShareLock);
	index_close(base_index, RowExclusiveLock);
	table_close(heap, AccessShareLock);
}

/* Main function of merger bgwroker */
void
lsm3_merger_main(Datum arg)
{
	Lsm3DictEntry* entry = (Lsm3DictEntry*)DatumGetPointer(arg);
	char	   *appname;

	pqsignal(SIGINT,  lsm3_merge_cancel);
	pqsignal(SIGQUIT, lsm3_merge_cancel);
	pqsignal(SIGTERM, lsm3_merge_cancel);

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	appname = psprintf("lsm3 merger for %d", entry->base);
	pgstat_report_appname(appname);
	pfree(appname);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	while (!Lsm3Cancel)
	{
		int merge_index= -1;
		int wr;
		pgstat_report_activity(STATE_IDLE, "waiting");
		wr = WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1L, PG_WAIT_EXTENSION);

		if (wr & WL_POSTMASTER_DEATH)
		{
			break;
		}

		ResetLatch(MyLatch);

		/* Check if merge is reuested under spinlock */
		SpinLockAcquire(&entry->spinlock);
		if (entry->start_merge)
		{
			merge_index = 1 - entry->active_index;
			entry->start_merge = false;
		}
		SpinLockRelease(&entry->spinlock);

		if (merge_index >= 0)
		{
			pgstat_report_activity(STATE_RUNNING, "merging");
			lsm3_merge_indexes(entry->base, entry->top[merge_index], entry->heap);
			pgstat_report_activity(STATE_RUNNING, "truncate");
			lsm3_truncate_index(entry->top[merge_index], entry->heap);
			SpinLockAcquire(&entry->spinlock);
			entry->merge_in_progress = false;
			SpinLockRelease(&entry->spinlock);
		}
	}
	entry->merger = NULL;
}

/* Build index tuple comparator context */
static SortSupport
lsm3_build_sortkeys(Relation index)
{
	int	keysz = IndexRelationGetNumberOfKeyAttributes(index);
	SortSupport	sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));
	BTScanInsert inskey = _bt_mkscankey(index, NULL);

	for (int i = 0; i < keysz; i++)
	{
		SortSupport sortKey = &sortKeys[i];
		ScanKey		scanKey = &inskey->scankeys[i];
		int16		strategy;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scanKey->sk_collation;
		sortKey->ssup_nulls_first =
			(scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
		sortKey->ssup_attno = scanKey->sk_attno;
		/* Abbreviation is not supported here */
		sortKey->abbreviate = false;

		AssertState(sortKey->ssup_attno != 0);

		strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
			BTGreaterStrategyNumber : BTLessStrategyNumber;

		PrepareSortSupportFromIndexRel(index, strategy, sortKey);
	}
	return sortKeys;
}

/* Compare index tuples */
static int
lsm3_compare_index_tuples(IndexScanDesc scan1, IndexScanDesc scan2, SortSupport sortKeys)
{
	int n_keys = IndexRelationGetNumberOfKeyAttributes(scan1->indexRelation);

	for (int i = 1; i <= n_keys; i++)
	{
		Datum	datum[2];
		bool	isNull[2];
		int 	result;

		datum[0] = index_getattr(scan1->xs_itup, i, scan1->xs_itupdesc, &isNull[0]);
		datum[1] = index_getattr(scan2->xs_itup, i, scan2->xs_itupdesc, &isNull[1]);
		result = ApplySortComparator(datum[0], isNull[0],
									 datum[1], isNull[1],
									 &sortKeys[i - 1]);
		if (result != 0)
		{
			return result;
		}
	}
	return ItemPointerCompare(&scan1->xs_heaptid, &scan2->xs_heaptid);
}

/*
 * Lsm3 access methods implementation
 */

static IndexBuildResult *
lsm3_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	LWLockAcquire(Lsm3DictLock, LW_EXCLUSIVE); /* Obtain exclusive lock on dictionary: it will be released in utility hook */
	Lsm3Entry = hash_search(Lsm3Dict, &RelationGetRelid(index), HASH_ENTER, NULL); /* Setting Lsm3Entry indicates to utility hook that Lsm index was created */
	lsm3_init_entry(Lsm3Entry, RelationGetRelid(heap));
	return btbuild(heap, index, indexInfo);
}

static void
lsm3_buildempty(Relation index)
{
	elog(ERROR, "Lsm3: unlogged Lsm2 indexes are not supported");
}

/* Insert in active top index, on overflow swap active indexes and initiate merge to base index */
static bool
lsm3_insert(Relation rel, Datum *values, bool *isnull,
			ItemPointer ht_ctid, Relation heapRel,
			IndexUniqueCheck checkUnique,
			IndexInfo *indexInfo)
{
	Lsm3DictEntry* entry = lsm3_get_entry(rel);
	bool ok;
	int active_index;
	uint64 n_merges;
	Relation index;

	/* Obtai current active index and increment access counter under spinock */
	SpinLockAcquire(&entry->spinlock);
	active_index = entry->active_index;
	entry->access_count[active_index] += 1;
	n_merges = entry->n_merges;
	SpinLockRelease(&entry->spinlock);

	/* Do insert in top index */
	index = index_open(entry->top[active_index], RowExclusiveLock);
	ok = btinsert(index, values, isnull, ht_ctid, heapRel, checkUnique, indexInfo);
	index_close(index, RowExclusiveLock);

	if (ok)
	{
		/* Otimization: do not check for overflow if merge was already intiated */
		bool overflow = !entry->merge_in_progress && lsm3_get_height(rel) > Lsm3TopIndexHeight;
		SpinLockAcquire(&entry->spinlock);
		/* If merge was not initiated before by somebody else, then do it */
		if (overflow && !entry->merge_in_progress && entry->n_merges == n_merges)
		{
			entry->merge_in_progress = true;
			entry->active_index ^= 1; /* swap top indexes */
			entry->n_merges += 1;
		}
		entry->access_count[active_index] -= 1;
		/* If all inserts in previous active index are completed then we can start merge */
		if (entry->merge_in_progress && entry->active_index != active_index && entry->access_count[active_index] == 0)
		{
			entry->start_merge = true;
			if (entry->merger == NULL) /* lazy start of bgworker */
			{
				lsm3_launch_bgworker(entry);
			}
			SetLatch(&entry->merger->procLatch);
		}
		SpinLockRelease(&entry->spinlock);
	}
	return ok;
}

static IndexScanDesc
lsm3_beginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	Lsm3ScanOpaque* so;
	int i;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->xs_itupdesc = RelationGetDescr(rel);
	so = (Lsm3ScanOpaque*)palloc(sizeof(Lsm3ScanOpaque));
	so->entry = lsm3_get_entry(rel);
	so->sortKeys = lsm3_build_sortkeys(rel);
	for (i = 0; i < 2; i++)
	{
		so->top_index[i] = index_open(so->entry->top[i], AccessShareLock);
		so->scan[i] = btbeginscan(so->top_index[i], nkeys, norderbys);
	}
	so->scan[3] = btbeginscan(rel, nkeys, norderbys);
	for (i = 0; i < 3; i++)
	{
		so->eof[i] = false;
		so->scan[i]->xs_want_itup = true;
	}
	scan->opaque = so;

	return scan;
}

static void
lsm3_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;

	for (int i = 0; i < 3; i++)
	{
		btrescan(so->scan[i], scankey, nscankeys, orderbys, norderbys);
		so->eof[i] = false;
	}
}

static void
lsm3_endscan(IndexScanDesc scan)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;

	for (int i = 0; i < 3; i++)
	{
		btendscan(so->scan[i]);
	}
	pfree(so);
}


static bool
lsm3_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*) scan->opaque;
	int min = -1;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;
	Assert(!scan->xs_want_itup);

	for (int i = 0; i < 3; i++)
	{
		BTScanOpaque bto = (BTScanOpaque) scan->opaque;
		if (!so->eof[i] && !BTScanPosIsValid(bto->currPos))
		{
			so->eof[i] = !_bt_first(so->scan[i], dir);
		}
		if (!so->eof[i])
		{
			if (min < 0)
			{
				min = i;
			}
			else
			{
				int result = lsm3_compare_index_tuples(so->scan[i], so->scan[min], so->sortKeys);
				if (result == 0)
				{
					/* DuplicateL it can happen during merge when same tid is both in top and base index */
					so->eof[i] = !_bt_next(so->scan[i], dir); /* just skip one of entries */
				}
				else if ((result < 0) == ScanDirectionIsForward(dir))
				{
					min = i;
				}
			}
		}
	}
	if (min < 0) /* all indexes are traversed */
	{
		return false;
	}
	else
	{
		scan->xs_heaptid = so->scan[min]->xs_heaptid; /* copy TID */
		so->eof[min] = !_bt_next(so->scan[min], dir); /* move forward index with minimal element */
		return true;
	}
}

static int64
lsm3_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	Lsm3ScanOpaque* so = (Lsm3ScanOpaque*)scan->opaque;	
	int64 ntids = btgetbitmap(so->scan[2], tbm);
	for (int i = 0; i < 2; i++)
	{
		Relation top_index = index_open(so->entry->top[i], AccessShareLock);
		ntids += btgetbitmap(so->scan[i], tbm);
		index_close(top_index, AccessShareLock);
	}
	return ntids;
}


Datum
lsm3_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;   /* We can't check that index is unique without accessing base index */
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false; /* TODO: not sure if it will work correctly with merge */
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false; /* TODO: parallel scac is not supported yet */
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm3_build;
	amroutine->ambuildempty = lsm3_buildempty;
	amroutine->aminsert = lsm3_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = lsm3_beginscan;
	amroutine->amrescan = lsm3_rescan;
	amroutine->amgettuple = lsm3_gettuple;
	amroutine->amgetbitmap = lsm3_getbitmap;
	amroutine->amendscan = lsm3_endscan;
	amroutine->ammarkpos = NULL;  /*  When do we need index_markpos? Can we live without it? */
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Access methods for B-Tree wrapper: actually we aonly want to disable inserts.
 */
static bool
lsm3_dummy_insert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  IndexInfo *indexInfo)
{
	return true;
}

Datum
lsm3_btree_wrapper(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = btbuild;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm3_dummy_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Utulity hook handling creation of Lsm3 indexes
 */
static void
lsm3_process_utility(PlannedStmt *plannedStmt,
					 const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo paramListInfo,
					 QueryEnvironment *queryEnvironment,
					 DestReceiver *destReceiver,
#if PG_VERSION_NUM>=130000
					 QueryCompletion *completionTag
#else
	                 char *completionTag
#endif
	)
{
    Node *parseTree = plannedStmt->utilityStmt;
	Lsm3Entry = NULL;
	(PreviousProcessUtilityHook ? PreviousProcessUtilityHook : standard_ProcessUtility)
		(plannedStmt,
		 queryString,
		 context,
		 paramListInfo,
		 queryEnvironment,
		 destReceiver,
		 completionTag);

	if (Lsm3Entry) /* This is Lsm3 creation statement */
	{
		int i;
		IndexStmt* stmt = (IndexStmt*)parseTree;
		char* originIndexName = stmt->idxname;
		char* originAccessMethod = stmt->accessMethod;

		for (i = 0; i < 2; i++)
		{
			stmt->accessMethod = "lsm3_btree_wrapper";
			stmt->idxname = psprintf("%s_top%d", originIndexName, i);
			Lsm3Entry->top[i] = DefineIndex(Lsm3Entry->base,
											stmt,
											InvalidOid,
											InvalidOid,
											InvalidOid,
											false,
											false,
											false,
											true,
											true).objectId;

			/*  Mark top index as invlid to prevent planner from using it in queries */
			index_set_state_flags(Lsm3Entry->top[i], INDEX_DROP_CLEAR_VALID);
		}
		stmt->accessMethod = originAccessMethod;
		stmt->idxname = originIndexName;
		LWLockRelease(Lsm3DictLock); /* Release lock set by lsm3_build */
	}
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "Lsm3: this extension should be loaded via shared_preload_libraries");
	}
	DefineCustomIntVariable("lsm3.top_index_height",
                            "Maximal height top index B-Tree.",
							NULL,
							&Lsm3TopIndexHeight,
							3,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("lsm3.max_indexes",
                            "Maximal number of Lsm3 indexes.",
							NULL,
							&Lsm3MaxIndexes,
							1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	RequestAddinShmemSpace(hash_estimate_size(Lsm3MaxIndexes, sizeof(Lsm3DictEntry)));
	RequestNamedLWLockTranche("lsm3", 1);

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = lsm3_shmem_startup;
	PreviousProcessUtilityHook = ProcessUtility_hook;
    ProcessUtility_hook = lsm3_process_utility;
}

void _PG_fini(void)
{
    ProcessUtility_hook = PreviousProcessUtilityHook;
	shmem_startup_hook = PreviousShmemStartupHook;
}
