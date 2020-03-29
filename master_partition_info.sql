-----------Master table
Create table aws_utility.master_partition_info
(
schema_name varchar,
table_name varchar,
partitioning_type varchar,
interval_value varchar,
interval_condition varchar,
created_date date DEFAULT NOW(),
updated_date date DEFAULT NOW()
);

------query to populate data into Master table

---master_partition_info query
--Run the query in oracle & export the output as insert scripts and execute them in postgres
SELECT SCHEMA_NAME,
       table_name,
       partitioning_type,
       value1 AS interval_value,
       TRIM( BOTH '''' FROM value2) AS interval_condition 
 FROM (
SELECT DTP.OWNER AS SCHEMA_NAME,
       DTP.OBJECT_NAME AS table_name,
       partitioning_type,
       interval,
       substr(interval,instr(interval,'(')+1,(instr(interval,',')-instr(interval,'('))-1) as value1,
       substr(interval,instr(interval,',')+2,(LENGTH(INTERVAL)- instr(interval,',')-3)) as value2 
FROM DBA_PART_TABLES DPT,DBA_OBJECTS DTP 
WHERE DPT.table_name=DTP.OBJECT_NAME 
AND DTP.OBJECT_TYPE='TABLE'
AND interval IS NOT NULL
);

--------------

--sample insert scripts for sample tables

insert into aws_utility.master_partition_info (schema_name,table_name,partitioning_type,interval_value,interval_condition) values ('partitions_test_schema','malmrablgmst','range','1','MONTH');
insert into aws_utility.master_partition_info (schema_name,table_name,partitioning_type,interval_value,interval_condition) values ('partitions_test_schema','stgmalmrablgmst','range','1','MONTH');
insert into aws_utility.master_partition_info (schema_name,table_name,partitioning_type,interval_value,interval_condition) values ('partitions_test_schema','test_partition_interval_year','range','1','YEAR');
insert into aws_utility.master_partition_info (schema_name,table_name,partitioning_type,interval_value,interval_condition) values ('partitions_test_schema','test_partition_interval_quarter','range','3','MONTH');


---sample commands

select aws_utility.create_rolling_partitions('partitions_test_schema','malmrablgmst',12);
select aws_utility.create_rolling_partitions('partitions_test_schema','malmrablgmst',12,current_date-30);
select aws_utility.create_rolling_partitions('partitions_test_schema','malmrablgmst',12,current_date+60);
select aws_utility.create_rolling_partitions('partitions_test_schema','test_partition_interval_year',12,current_date-60)
select aws_utility.create_rolling_partitions('partitions_test_schema','test_partition_interval_year',12,current_date)





