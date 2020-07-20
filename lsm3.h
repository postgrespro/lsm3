/*
 * Control structure for Lsm3 index located in sharedmemory
 */
typedef struct
{
	Oid base;   /* Oid of base index */
	Oid heap;   /* Oid of indexed relation */
	Oid top[2]; /* Oids of two top indexes */
	int access_count[2]; /* Access counter for top indexes */
	int active_index; /* Index used for insert */
	uint64 n_merges;  /* Number of performed merges */
	bool start_merge; /* Start merging of top index with base index */
	bool merge_in_progress; /* Overflow of top index intiate merge process */
	PGPROC* merger;   /* Merger background worker */
	slock_t spinlock; /* Spinlock to synchronize access */
} Lsm3DictEntry;

/*
 * Opaque part of index scan descriptor
 */
typedef struct
{
	Lsm3DictEntry* entry;    /* Lsm3 control structure */
	Relation 	   top_index[2]; /* Opened top index relations */
	SortSupport    sortKeys; /* Context for comparing index tuples */
	IndexScanDesc  scan[3];  /* Scan descriptors for two top indexes and base index */
	bool           eof[3];   /* Indicators that end of index was reached */
} Lsm3ScanOpaque;
