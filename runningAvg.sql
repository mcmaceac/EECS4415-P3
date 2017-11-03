-- ===========================================================================
-- pipelined sum by partition
--     This is use of a user-defined aggregate function, but in an "inverted"
--     way to usual.  It aggregates over a composite type!  The composite
--     type is passing in the INT to add to the sum as well as a boolean
--     flag indicating whether the tuple is the start of a partition (and
--     thus the running sum needs to be reset).  Interestingly, we only
--     need an INT as internal state for the aggregate function.
--
--     This is 100% pipelined; there is no group by!  And all
--     order-by's are with respect to the same criterion: id asc.
-- ---------------------------------------------------------------------------
-- created  : 2017-07-31
-- authors  : Mark Dogfury
-- platform : PostgreSQL 9.6
-- ===========================================================================

\pset pager off

-- ===========================================================================
-- create a trial table, populate it, and run a test query
-- ---------------------------------------------------------------------------
-- TABLE Stream
\echo -- creating table "Stream"

drop table if exists Stream;
create table Stream (
    id      int,
    grp     int,
    measure int,
    constraint streamPK
        primary key (id),
    constraint idNotNeg
        check (id >= 0),
    constraint grpNotNeg
        check (grp >= 0)
);

-- ---------------------------------------------------------------------------
-- POPULATE: add some tuples to table Stream
\echo -- populating "Stream"

insert into Stream values
    ( 0, 0,  2),
    ( 1, 0,  3),
    ( 2, 1,  5),
    ( 3, 1,  7),
    ( 4, 1, 11),
    ( 5, 0, 13),
    ( 6, 0, 17),
    ( 7, 0, 19),
    ( 8, 0, 23),
    ( 9, 2, 29),
    (10, 2, 31),
    (11, 5, 37),
    (12, 3, 41),
    (13, 3, 43);

-- ===========================================================================
-- define a running sum that restarts on partition boundaries
--    1. we use a composite type to pass in consisting of
--           - the measure (int) that we are summing, and
--           - a boolean flag that indicates the start of a partition
--    2. state function      : the accumulator
--    3. finish function     : provides the return value
--    4. agggregate function : the wrapper
-- ---------------------------------------------------------------------------
-- intRec : a composite type that has an int and a boolean flag
\echo -- creating composite type "intRec"

drop type if exists
    intRec
    cascade;
create type intRec as (
    number  int,
    restart boolean
);

-- ---------------------------------------------------------------------------
-- runningSum_state : accumulator function
\echo -- creating function "runningSum_state"

drop function if exists
    runningSum_state(int, intRec)
    cascade;
create function runningSum_state(int, intRec)
returns int
language plpgsql
as $f$
    declare i alias for $1;
    declare a alias for $2;
    declare j int;
    begin
        if a.restart or i is null then
            j := a.number;
        elsif a.number is null then
            j := i;
        else
            j := a.number + i;
        end if;
        return j;
    end
$f$;

-- ---------------------------------------------------------------------------
-- runningSum_final : returns the aggregate value
\echo -- creating function "runningSum_final"

drop function if exists
    runningSum_final(int)
    cascade;
create function runningSum_final(int)
returns intRec
language sql
as $f$
    select cast(($1, false) as intRec);
$f$;

-- ---------------------------------------------------------------------------
-- runningSum : the aggregate function
\echo -- creating aggregate function "runningSum"

drop aggregate if exists
    runningSum(intRec)
    cascade;
create aggregate runningSum(intRec) (
    sfunc     = runningSum_state,
    stype     = int,
    finalfunc = runningSum_final
);

-- ---------------------------------------------------------------------------
-- pipeline sliging-window query that uses our agggregate function
\echo -- querying "Stream" with running sum

with
    -- look at the neighbour tuple to the left to fetch its grp value
    CellLeft (id, grp, measure, lft) as (
        select  id,
                grp,
                measure,
                coalesce(
                    max(grp) over (
                        order by id
                        rows between
                        1 preceding
                            and
                        1 preceding ),
                    -1 )
        from Stream
    ),
    -- determine whether current tuple is start of a group
    CellStart(id, grp, measure, start) as (
        select  id,
                grp,
                measure,
                cast(
                    case
                    when grp = lft then 0
                    else                1
                    end
                as boolean)
        from CellLeft
    ),
    -- bundle the measure and start-flag into an intRC
    CellFlag(id, grp, intRC) as (
        select  id,
                grp,
                cast((measure, start) as intRec)
        from CellStart
    ),
    -- call our runningSum aggregator
    CellRun(id, grp, measure, runningRC) as (
        select  id,
                grp,
                (intRC).number,
                runningSum(intRC)
                    over (order by id)
        from CellFlag
    ),
    -- extract the running sum from the composite
    CellAggr(id, grp, measure, running) as (
        select  id,
                grp,
                measure,
                (runningRC).number
        from CellRun
    )
-- report
select id, grp, measure, running
from CellAggr
order by id;

