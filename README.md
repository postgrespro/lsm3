LSM tree implemented using standard Postgres B-Tree indexes.
Top index is used to perform fast inserts and on overflow it is merged
with base index. To perform merge operation concurrently
without blocking other operations with index, two top indexes are used:
active and merged. So totally there are three B-Tree indexes:
two top indexes and base index.
When performing index scan we have to merge scans of all this three indexes.

This extensions needs to create data structure in shared memory and this is why it should be loaded through
"shared_preload_library" list. Once extension is created, you can define indexes using lsm3 access method:

```sql
create extension lsm3;
create table t(id integer, val text);
create index idx on t using lsm3(id);
```

`Lsm3` provides for the same types and set of operations as standard B-Tree.

Current restrictions of `Lsm3`:
- Parallel index scan is not supported.
- Array keys are not supported.
- `Lsm3` index can not be declared as unique.

`Lsm3` extension can be configured using the following parameters:
- `lsm3.max_indexes`: maximal number of Lsm3 indexes (default 1024).
- `lsm3.max_top_index_size`: Maximal size (kb) of top index (default 64Mb).

