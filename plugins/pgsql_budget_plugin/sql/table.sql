CREATE TABLE IF NOT EXISTS b_motions(
    motion_serial      BIGSERIAL   PRIMARY KEY,
    motion_id        INTEGER,
    root_id          INTEGER,
    title            TEXT,
    content          TEXT,
    quantity         TEXT,
    proposer         TEXT,
    section          INTEGER,
    takecoin_num     INTEGER,
    approve_end_block_num  INTEGER,
    extern_data      TEXT,
    motion_type         INTEGER
)WITH ( OIDS=FALSE );

CREATE TABLE IF NOT EXISTS b_members(
    member_serial      SERIAL   PRIMARY KEY,
    member  TEXT[]
)WITH ( OIDS=FALSE );

CREATE TABLE IF NOT EXISTS b_approves(
   proposer    TEXT,
   id        INTEGER,
   requested          TEXT[],
   approved            TEXT[],
   unapproved          TEXT[],
   approve_type         INTEGER,
   PRIMARY KEY(proposer, id)
)WITH ( OIDS=FALSE );

CREATE TABLE IF NOT EXISTS b_takecoins(
   proposer    TEXT,
   id        INTEGER,
   motion_id        INTEGER,
   content          TEXT,
   quantity         TEXT,
   receiver          TEXT,
   section           INTEGER,
   end_block_num     INTEGER,
   requested          TEXT[],
   approved            TEXT[],
   unapproved          TEXT[],
   takecoin_type        INTEGER,
   PRIMARY KEY(proposer, id)
)WITH ( OIDS=FALSE );


CREATE TABLE IF NOT EXISTS t_test(
    id      BIGSERIAL   PRIMARY KEY,
    memo       TEXT
)WITH ( OIDS=FALSE );

/*
delete from b_motions;
delete from b_members;
delete from b_approves;
delete from b_takecoins;
*/
-- 
-- 
-- 