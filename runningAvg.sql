-- created  : 2017-11-04
-- authors  : Matthew MacEachern
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
    number  float,
    restart boolean
);

-- stateCount : a composite type that has two ints, one for the state and one for the i value
\echo -- creating composite type "stateCount"
drop type if exists
    stateCount
    cascade;
create type stateCount as (
    state float,
    count int
);

-- ---------------------------------------------------------------------------
-- runningAvg_state : accumulator function
\echo -- creating function "runningAvg_state"

drop function if exists
    runningAvg_state(stateCount, intRec)
    cascade;
create function runningAvg_state(stateCount, intRec)
returns stateCount
language plpgsql
as $f$
    declare i alias for $1;
    declare a alias for $2;
    declare j stateCount;
    begin
        if a.restart or i is null then  --beginning of a group or stream
            j.state := a.number;
            j.count := 1;
        elsif a.number is null then
            j.state := i.state;
            j.count := 1;
        else
            j.count := i.count + 1;
            j.state := i.state + (a.number - i.state)/j.count;
        end if;
        return j;
    end
$f$;

-- ---------------------------------------------------------------------------
-- runningAvg_final : returns the aggregate value
\echo -- creating function "runningAvg_final"

drop function if exists
    runningAvg_final(stateCount)
    cascade;
create function runningAvg_final(stateCount)
returns intRec
language sql
as $f$
    select cast(($1.state, false) as intRec);
$f$;

-- ---------------------------------------------------------------------------
-- runningAvg : the aggregate function
\echo -- creating aggregate function "runningAvg"

drop aggregate if exists
    runningAvg(intRec)
    cascade;
create aggregate runningAvg(intRec) (
    sfunc     = runningAvg_state,
    stype     = stateCount,
    finalfunc = runningAvg_final
);

-- ---------------------------------------------------------------------------
-- pipeline sliding-window query that uses our aggregate function
\echo -- querying "Stream" with running avg

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
    -- call our runningAvg aggregator
    CellRun(id, grp, measure, runningRC) as (
        select  id,
                grp,
                (intRC).number,
                runningAvg(intRC)
                    over (order by id)
        from CellFlag
    ),
    -- extract the running avg from the composite
    CellAggr(id, grp, measure, average) as (
        select  id,
                grp,
                measure,
                (runningRC).number
        from CellRun
    )
-- report
select id, grp, measure, average
from CellAggr
order by id;

