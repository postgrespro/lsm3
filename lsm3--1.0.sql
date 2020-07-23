-- Lsm3 operators

CREATE OR REPLACE FUNCTION lsm3_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD lsm3 TYPE INDEX HANDLER lsm3_handler;

CREATE OPERATOR FAMILY integer_ops USING lsm3;

CREATE OPERATOR CLASS int2_ops DEFAULT
	FOR TYPE int2 USING lsm3 FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2,int2);

CREATE OPERATOR CLASS int4_ops DEFAULT
	FOR TYPE int4 USING lsm3 FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4,int4);

CREATE OPERATOR CLASS int8_ops DEFAULT
	FOR TYPE int8 USING lsm3 FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8,int8);

ALTER OPERATOR FAMILY integer_ops USING lsm3 ADD
	OPERATOR 1  < (int2,int4),
	OPERATOR 1  < (int2,int8),
	OPERATOR 1  < (int4,int2),
	OPERATOR 1  < (int4,int8),
	OPERATOR 1  < (int8,int2),
	OPERATOR 1  < (int8,int4),

	OPERATOR 2  <= (int2,int4),
	OPERATOR 2  <= (int2,int8),
	OPERATOR 2  <= (int4,int2),
	OPERATOR 2  <= (int4,int8),
	OPERATOR 2  <= (int8,int2),
	OPERATOR 2  <= (int8,int4),

	OPERATOR 3  = (int2,int4),
	OPERATOR 3  = (int2,int8),
	OPERATOR 3  = (int4,int2),
	OPERATOR 3  = (int4,int8),
	OPERATOR 3  = (int8,int2),
	OPERATOR 3  = (int8,int4),

	OPERATOR 4  >= (int2,int4),
	OPERATOR 4  >= (int2,int8),
	OPERATOR 4  >= (int4,int2),
	OPERATOR 4  >= (int4,int8),
	OPERATOR 4  >= (int8,int2),
	OPERATOR 4  >= (int8,int4),

	OPERATOR 5  > (int2,int4),
	OPERATOR 5  > (int2,int8),
	OPERATOR 5  > (int4,int2),
	OPERATOR 5  > (int4,int8),
	OPERATOR 5  > (int8,int2),
	OPERATOR 5  > (int8,int4),

	FUNCTION 1(int2,int4)  btint24cmp(int2,int4),
	FUNCTION 1(int2,int8)  btint28cmp(int2,int8),
	FUNCTION 1(int4,int2)  btint42cmp(int4,int2),
	FUNCTION 1(int4,int8)  btint48cmp(int4,int8),
	FUNCTION 1(int8,int4)  btint84cmp(int8,int4),
	FUNCTION 1(int8,int2)  btint82cmp(int8,int2),

	FUNCTION 2(int2,int2)  btint2sortsupport(internal),
	FUNCTION 2(int4,int4)  btint4sortsupport(internal),
	FUNCTION 2(int8,int8)  btint8sortsupport(internal),

    FUNCTION 3(int2,int8)  in_range(int2,int2,int8,bool,bool),
    FUNCTION 3(int2,int4)  in_range(int2,int2,int4,bool,bool),
    FUNCTION 3(int2,int2)  in_range(int2,int2,int2,bool,bool),
    FUNCTION 3(int4,int8)  in_range(int4,int4,int8,bool,bool),
    FUNCTION 3(int4,int4)  in_range(int4,int4,int4,bool,bool),
    FUNCTION 3(int4,int2)  in_range(int4,int4,int2,bool,bool),
    FUNCTION 3(int8,int8)  in_range(int8,int8,int8,bool,bool),

    FUNCTION 4(int2,int2)  btequalimage(oid),
    FUNCTION 4(int4,int4)  btequalimage(oid),
    FUNCTION 4(int8,int8)  btequalimage(oid);

CREATE OPERATOR FAMILY float_ops USING lsm3;

CREATE OPERATOR CLASS float4_ops DEFAULT
	FOR TYPE float4 USING lsm3 FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat4cmp(float4,float4);

CREATE OPERATOR CLASS float8_ops DEFAULT
	FOR TYPE float8 USING lsm3 FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat8cmp(float8,float8);


ALTER OPERATOR FAMILY float_ops USING lsm3 ADD
	OPERATOR 1  < (float4,float8),
	OPERATOR 1  < (float8,float4),

	OPERATOR 2  <= (float4,float8),
	OPERATOR 2  <= (float8,float4),

	OPERATOR 3  = (float4,float8),
	OPERATOR 3  = (float8,float4),

	OPERATOR 4  >= (float4,float8),
	OPERATOR 4  >= (float8,float4),

	OPERATOR 5  > (float4,float8),
	OPERATOR 5  > (float8,float4),

	FUNCTION 1(float4,float8)  btfloat48cmp(float4,float8),
	FUNCTION 1(float8,float4)  btfloat84cmp(float8,float4),

	FUNCTION 2(float4,float4)  btfloat4sortsupport(internal),
	FUNCTION 2(float8,float8)  btfloat8sortsupport(internal),

    FUNCTION 3(float4,float8)  in_range(float4,float4,float8,bool,bool),
    FUNCTION 3(float8,float8)  in_range(float8,float8,float8,bool,bool);

-- lsm3_bree_wrapper operators

CREATE OR REPLACE FUNCTION lsm3_btree_wrapper(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD lsm3_btree_wrapper TYPE INDEX HANDLER lsm3_btree_wrapper;

CREATE OPERATOR FAMILY integer_ops USING lsm3_btree_wrapper;

CREATE OPERATOR CLASS int2_ops DEFAULT
	FOR TYPE int2 USING lsm3_btree_wrapper FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2,int2);

CREATE OPERATOR CLASS int4_ops DEFAULT
	FOR TYPE int4 USING lsm3_btree_wrapper FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4,int4);

CREATE OPERATOR CLASS int8_ops DEFAULT
	FOR TYPE int8 USING lsm3_btree_wrapper FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8,int8);

ALTER OPERATOR FAMILY integer_ops USING lsm3_btree_wrapper ADD
	OPERATOR 1  < (int2,int4),
	OPERATOR 1  < (int2,int8),
	OPERATOR 1  < (int4,int2),
	OPERATOR 1  < (int4,int8),
	OPERATOR 1  < (int8,int2),
	OPERATOR 1  < (int8,int4),

	OPERATOR 2  <= (int2,int4),
	OPERATOR 2  <= (int2,int8),
	OPERATOR 2  <= (int4,int2),
	OPERATOR 2  <= (int4,int8),
	OPERATOR 2  <= (int8,int2),
	OPERATOR 2  <= (int8,int4),

	OPERATOR 3  = (int2,int4),
	OPERATOR 3  = (int2,int8),
	OPERATOR 3  = (int4,int2),
	OPERATOR 3  = (int4,int8),
	OPERATOR 3  = (int8,int2),
	OPERATOR 3  = (int8,int4),

	OPERATOR 4  >= (int2,int4),
	OPERATOR 4  >= (int2,int8),
	OPERATOR 4  >= (int4,int2),
	OPERATOR 4  >= (int4,int8),
	OPERATOR 4  >= (int8,int2),
	OPERATOR 4  >= (int8,int4),

	OPERATOR 5  > (int2,int4),
	OPERATOR 5  > (int2,int8),
	OPERATOR 5  > (int4,int2),
	OPERATOR 5  > (int4,int8),
	OPERATOR 5  > (int8,int2),
	OPERATOR 5  > (int8,int4),

	FUNCTION 1(int2,int4)  btint24cmp(int2,int4),
	FUNCTION 1(int2,int8)  btint28cmp(int2,int8),
	FUNCTION 1(int4,int2)  btint42cmp(int4,int2),
	FUNCTION 1(int4,int8)  btint48cmp(int4,int8),
	FUNCTION 1(int8,int4)  btint84cmp(int8,int4),
	FUNCTION 1(int8,int2)  btint82cmp(int8,int2),

	FUNCTION 2(int2,int2)  btint2sortsupport(internal),
	FUNCTION 2(int4,int4)  btint4sortsupport(internal),
	FUNCTION 2(int8,int8)  btint8sortsupport(internal),

    FUNCTION 3(int2,int8)  in_range(int2,int2,int8,bool,bool),
    FUNCTION 3(int2,int4)  in_range(int2,int2,int4,bool,bool),
    FUNCTION 3(int2,int2)  in_range(int2,int2,int2,bool,bool),
    FUNCTION 3(int4,int8)  in_range(int4,int4,int8,bool,bool),
    FUNCTION 3(int4,int4)  in_range(int4,int4,int4,bool,bool),
    FUNCTION 3(int4,int2)  in_range(int4,int4,int2,bool,bool),
    FUNCTION 3(int8,int8)  in_range(int8,int8,int8,bool,bool),

    FUNCTION 4(int2,int2)  btequalimage(oid),
    FUNCTION 4(int4,int4)  btequalimage(oid),
    FUNCTION 4(int8,int8)  btequalimage(oid);

CREATE OPERATOR FAMILY float_ops USING lsm3_btree_wrapper;

CREATE OPERATOR CLASS float4_ops DEFAULT
	FOR TYPE float4 USING lsm3_btree_wrapper FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat4cmp(float4,float4);

CREATE OPERATOR CLASS float8_ops DEFAULT
	FOR TYPE float8 USING lsm3_btree_wrapper FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat8cmp(float8,float8);


ALTER OPERATOR FAMILY float_ops USING lsm3_btree_wrapper ADD
	OPERATOR 1  < (float4,float8),
	OPERATOR 1  < (float8,float4),

	OPERATOR 2  <= (float4,float8),
	OPERATOR 2  <= (float8,float4),

	OPERATOR 3  = (float4,float8),
	OPERATOR 3  = (float8,float4),

	OPERATOR 4  >= (float4,float8),
	OPERATOR 4  >= (float8,float4),

	OPERATOR 5  > (float4,float8),
	OPERATOR 5  > (float8,float4),

	FUNCTION 1(float4,float8)  btfloat48cmp(float4,float8),
	FUNCTION 1(float8,float4)  btfloat84cmp(float8,float4),

	FUNCTION 2(float4,float4)  btfloat4sortsupport(internal),
	FUNCTION 2(float8,float8)  btfloat8sortsupport(internal),

    FUNCTION 3(float4,float8)  in_range(float4,float4,float8,bool,bool),
    FUNCTION 3(float8,float8)  in_range(float8,float8,float8,bool,bool);
