---create sample schema
create schema partitions_test_schema;

---create sample tables

CREATE TABLE partitions_test_schema.malmrablgmst(
    cmpcod CHARACTER VARYING(6) NOT NULL,
    malseqnum NUMERIC(10,0) NOT NULL,
    malidr CHARACTER VARYING(29),
    csgdocnum CHARACTER VARYING(35),
    csgseqnum NUMERIC(5,0) NOT NULL DEFAULT 0,
    poacod CHARACTER VARYING(5),
    uldnum CHARACTER VARYING(12),
    orgarpcod CHARACTER VARYING(4),
    dstarpcod CHARACTER VARYING(4),
    orgctycod CHARACTER VARYING(4),
    dstctycod CHARACTER VARYING(4),
    orgexgofc CHARACTER VARYING(6),
    dstexgofc CHARACTER VARYING(6),
    malctgcod CHARACTER VARYING(1),
    malsubcls CHARACTER VARYING(2),
    rsn CHARACTER VARYING(3),
    dsn CHARACTER VARYING(4),
    hsn CHARACTER VARYING(1),
    yer NUMERIC(1,0) NOT NULL DEFAULT 0,
    regind CHARACTER VARYING(1),
    rou CHARACTER VARYING(100),
    totpcs NUMERIC(6,0) NOT NULL DEFAULT 0,
    grswgt NUMERIC(16,4) NOT NULL DEFAULT 0,
    mrasta CHARACTER VARYING(1),
    rcvdat TIMESTAMP(0) WITHOUT TIME ZONE,
    fstfltdat TIMESTAMP(0) WITHOUT TIME ZONE,
    poaflg CHARACTER VARYING(1) DEFAULT 'N',
    trfcarcod CHARACTER VARYING(8),
    domintflg CHARACTER VARYING(1),
    dclval NUMERIC(20,8) NOT NULL DEFAULT 0,
    curcod CHARACTER VARYING(3),
    malcmpcod CHARACTER VARYING(6),
    lstupdusr CHARACTER VARYING(50),
    lstupdtim TIMESTAMP(3) WITHOUT TIME ZONE,
    tagidx NUMERIC(1,0) NOT NULL DEFAULT 0,
    malsrvlvl CHARACTER VARYING(2),
    srvrspln CHARACTER VARYING(1) NOT NULL DEFAULT 'N',
    malsrc CHARACTER VARYING(6),
    scnwvdflg CHARACTER VARYING(6),
    malperflg CHARACTER VARYING(2),
    cdcflg CHARACTER VARYING(1) NOT NULL DEFAULT 'I',
    dwhlstupdtim TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT now()
)
    PARTITION BY RANGE (dwhlstupdtim)
        WITH (
        OIDS=FALSE
        );



CREATE TABLE partitions_test_schema.stgmalmrablgmst(
    cmpcod CHARACTER VARYING(6),
    malseqnum NUMERIC(10,0),
    malidr CHARACTER VARYING(29),
    csgdocnum CHARACTER VARYING(35),
    csgseqnum NUMERIC(5,0),
    poacod CHARACTER VARYING(5),
    uldnum CHARACTER VARYING(12),
    orgarpcod CHARACTER VARYING(4),
    dstarpcod CHARACTER VARYING(4),
    orgctycod CHARACTER VARYING(4),
    dstctycod CHARACTER VARYING(4),
    orgexgofc CHARACTER VARYING(6),
    dstexgofc CHARACTER VARYING(6),
    malctgcod CHARACTER VARYING(1),
    malsubcls CHARACTER VARYING(2),
    rsn CHARACTER VARYING(3),
    dsn CHARACTER VARYING(4),
    hsn CHARACTER VARYING(1),
    yer NUMERIC(1,0),
    regind CHARACTER VARYING(1),
    rou CHARACTER VARYING(500),
    totpcs NUMERIC(6,0),
    grswgt NUMERIC(16,4),
    mrasta CHARACTER VARYING(1),
    rcvdat TIMESTAMP(0) WITHOUT TIME ZONE,
    domintflg CHARACTER VARYING(1),
    dclval NUMERIC(16,4),
    curcod CHARACTER VARYING(3),
    fstfltdat TIMESTAMP(0) WITHOUT TIME ZONE,
    lstupdusr CHARACTER VARYING(50),
    lstupdtim TIMESTAMP(3) WITHOUT TIME ZONE,
    dwhtim TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL,
    dwhflg CHARACTER VARYING(1) NOT NULL,
    dwhextseq DOUBLE PRECISION NOT NULL,
    docowridr NUMERIC(5,0),
    mstdocnum CHARACTER VARYING(20),
    dupnum NUMERIC(2,0),
    seqnum NUMERIC(5,0),
    shppfx CHARACTER VARYING(4),
    poaflg CHARACTER VARYING(1),
    trfcarcod CHARACTER VARYING(8),
    malcmpcod CHARACTER VARYING(6),
    malsrvlvl CHARACTER VARYING(2),
    srvrspln CHARACTER VARYING(1),
    malsrc CHARACTER VARYING(6),
    scnwvdflg CHARACTER VARYING(6),
    malperflg CHARACTER VARYING(1)
)
    PARTITION BY RANGE (dwhtim)
        WITH (
        OIDS=FALSE
        );


CREATE TABLE partitions_test_schema.test_partition_interval_year(
    cmpcod CHARACTER VARYING(6) NOT NULL,
    malseqnum NUMERIC(10,0) NOT NULL,
    malidr CHARACTER VARYING(29),
    csgdocnum CHARACTER VARYING(35),
    csgseqnum NUMERIC(5,0) NOT NULL DEFAULT 0,
    poacod CHARACTER VARYING(5),
    uldnum CHARACTER VARYING(12),
    orgarpcod CHARACTER VARYING(4),
    dstarpcod CHARACTER VARYING(4),
    orgctycod CHARACTER VARYING(4),
    dstctycod CHARACTER VARYING(4),
    orgexgofc CHARACTER VARYING(6),
    dstexgofc CHARACTER VARYING(6),
    malctgcod CHARACTER VARYING(1),
    malsubcls CHARACTER VARYING(2),
    rsn CHARACTER VARYING(3),
    dsn CHARACTER VARYING(4),
    hsn CHARACTER VARYING(1),
    yer NUMERIC(1,0) NOT NULL DEFAULT 0,
    regind CHARACTER VARYING(1),
    rou CHARACTER VARYING(100),
    totpcs NUMERIC(6,0) NOT NULL DEFAULT 0,
    grswgt NUMERIC(16,4) NOT NULL DEFAULT 0,
    mrasta CHARACTER VARYING(1),
    rcvdat TIMESTAMP(0) WITHOUT TIME ZONE,
    fstfltdat TIMESTAMP(0) WITHOUT TIME ZONE,
    poaflg CHARACTER VARYING(1) DEFAULT 'N',
    trfcarcod CHARACTER VARYING(8),
    domintflg CHARACTER VARYING(1),
    dclval NUMERIC(20,8) NOT NULL DEFAULT 0,
    curcod CHARACTER VARYING(3),
    malcmpcod CHARACTER VARYING(6),
    lstupdusr CHARACTER VARYING(50),
    lstupdtim TIMESTAMP(3) WITHOUT TIME ZONE,
    tagidx NUMERIC(1,0) NOT NULL DEFAULT 0,
    malsrvlvl CHARACTER VARYING(2),
    srvrspln CHARACTER VARYING(1) NOT NULL DEFAULT 'N',
    malsrc CHARACTER VARYING(6),
    scnwvdflg CHARACTER VARYING(6),
    malperflg CHARACTER VARYING(2),
    cdcflg CHARACTER VARYING(1) NOT NULL DEFAULT 'I',
    dwhlstupdtim TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL DEFAULT aws_oracle_ext.SYSDATE()
)
    PARTITION BY RANGE (dwhlstupdtim)
        WITH (
        OIDS=FALSE
        );



CREATE TABLE partitions_test_schema.test_partition_interval_quarter(
    cmpcod CHARACTER VARYING(6),
    malseqnum NUMERIC(10,0),
    malidr CHARACTER VARYING(29),
    csgdocnum CHARACTER VARYING(35),
    csgseqnum NUMERIC(5,0),
    poacod CHARACTER VARYING(5),
    uldnum CHARACTER VARYING(12),
    orgarpcod CHARACTER VARYING(4),
    dstarpcod CHARACTER VARYING(4),
    orgctycod CHARACTER VARYING(4),
    dstctycod CHARACTER VARYING(4),
    orgexgofc CHARACTER VARYING(6),
    dstexgofc CHARACTER VARYING(6),
    malctgcod CHARACTER VARYING(1),
    malsubcls CHARACTER VARYING(2),
    rsn CHARACTER VARYING(3),
    dsn CHARACTER VARYING(4),
    hsn CHARACTER VARYING(1),
    yer NUMERIC(1,0),
    regind CHARACTER VARYING(1),
    rou CHARACTER VARYING(500),
    totpcs NUMERIC(6,0),
    grswgt NUMERIC(16,4),
    mrasta CHARACTER VARYING(1),
    rcvdat TIMESTAMP(0) WITHOUT TIME ZONE,
    domintflg CHARACTER VARYING(1),
    dclval NUMERIC(16,4),
    curcod CHARACTER VARYING(3),
    fstfltdat TIMESTAMP(0) WITHOUT TIME ZONE,
    lstupdusr CHARACTER VARYING(50),
    lstupdtim TIMESTAMP(3) WITHOUT TIME ZONE,
    dwhtim TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL,
    dwhflg CHARACTER VARYING(1) NOT NULL,
    dwhextseq DOUBLE PRECISION NOT NULL,
    docowridr NUMERIC(5,0),
    mstdocnum CHARACTER VARYING(20),
    dupnum NUMERIC(2,0),
    seqnum NUMERIC(5,0),
    shppfx CHARACTER VARYING(4),
    poaflg CHARACTER VARYING(1),
    trfcarcod CHARACTER VARYING(8),
    malcmpcod CHARACTER VARYING(6),
    malsrvlvl CHARACTER VARYING(2),
    srvrspln CHARACTER VARYING(1),
    malsrc CHARACTER VARYING(6),
    scnwvdflg CHARACTER VARYING(6),
    malperflg CHARACTER VARYING(1)
)
    PARTITION BY RANGE (dwhtim)
        WITH (
        OIDS=FALSE
        );



-- ------------ Write CREATE-PARTITION-stage scripts -----------

CREATE TABLE partitions_test_schema.test_partition_interval_year_p2
        PARTITION OF partitions_test_schema.test_partition_interval_year
        FOR VALUES FROM (MINVALUE) TO ('2020-01-01 00:00:00');
        
CREATE TABLE partitions_test_schema.test_partition_interval_year_p3
        PARTITION OF partitions_test_schema.test_partition_interval_year
        FOR VALUES from ('2020-01-01 00:00:00') to ('2021-01-01 00:00:00');   
        
  CREATE TABLE IF NOT EXISTS partitions_test_schema.test_partition_interval_year_default
        PARTITION OF  partitions_test_schema.test_partition_interval_year
        DEFAULT;



CREATE TABLE partitions_test_schema.test_partition_interval_quarter_p2
        PARTITION OF partitions_test_schema.test_partition_interval_quarter
        FOR VALUES FROM (MINVALUE) TO ('2020-01-01 00:00:00');
        
CREATE TABLE partitions_test_schema.test_partition_interval_quarter_p3
        PARTITION OF partitions_test_schema.test_partition_interval_quarter
        FOR VALUES from ('2020-01-01 00:00:00') to ('2020-04-01 00:00:00');   
        
 CREATE TABLE partitions_test_schema.test_partition_interval_quarter_p4
        PARTITION OF partitions_test_schema.test_partition_interval_quarter
        FOR VALUES from ('2020-04-01 00:00:00') to ('2020-07-01 00:00:00');         
        
  CREATE TABLE IF NOT EXISTS test_partition_interval_quarter_default
        PARTITION OF  partitions_test_schema.test_partition_interval_quarter
        DEFAULT;



CREATE TABLE partitions_test_schema.malmrablgmst_pos_data_p2
        PARTITION OF partitions_test_schema.malmrablgmst
        FOR VALUES FROM (MINVALUE) TO ('2020-01-01 00:00:00');
        
CREATE TABLE partitions_test_schema.malmrablgmst_pos_data_p3
        PARTITION OF partitions_test_schema.malmrablgmst
        FOR VALUES from ('2020-01-01 00:00:00') to ('2020-02-01 00:00:00');   
        
CREATE TABLE partitions_test_schema.malmrablgmst_pos_data_p4
        PARTITION OF partitions_test_schema.malmrablgmst
        FOR VALUES from ('2020-02-01 00:00:00') to ('2020-03-01 00:00:00');                 

CREATE TABLE IF NOT EXISTS partitions_test_schema.malmrablgmst_default
        PARTITION OF  partitions_test_schema.malmrablgmst
        DEFAULT;


CREATE TABLE  partitions_test_schema.stgmalmrablgmst_pos_data_p2
        PARTITION OF partitions_test_schema.stgmalmrablgmst
        FOR VALUES FROM (MINVALUE) TO ('2020-01-01 00:00:00');
        
CREATE TABLE partitions_test_schema.stgmalmrablgmst_pos_data_p3
        PARTITION OF partitions_test_schema.stgmalmrablgmst
        FOR VALUES from ('2020-01-01 00:00:00') to ('2020-02-01 00:00:00');   
        
CREATE TABLE partitions_test_schema.stgmalmrablgmst_pos_data_p4
        PARTITION OF partitions_test_schema.stgmalmrablgmst
        FOR VALUES from ('2020-02-01 00:00:00') to ('2020-03-01 00:00:00');                 

  CREATE TABLE IF NOT EXISTS partitions_test_schema.stgmalmrablgmst_default
        PARTITION OF  partitions_test_schema.stgmalmrablgmst
        DEFAULT;


-- ------------ Write CREATE-CONSTRAINT-stage scripts -----------

ALTER TABLE partitions_test_schema.malmrablgmst_pos_data_p2
ADD CONSTRAINT malmrablgmst_pk_pos_data_p2 PRIMARY KEY (malseqnum, cmpcod);



ALTER TABLE partitions_test_schema.stgmalmrablgmst
ADD CONSTRAINT stgmalmrablgmst_uk1 UNIQUE (cmpcod, malseqnum, dwhtim);


