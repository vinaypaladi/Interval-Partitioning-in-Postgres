CREATE OR REPLACE FUNCTION aws_utility.create_rolling_partitions(
     IN p_schema VARCHAR DEFAULT NULL,
     IN p_table_to_execute VARCHAR DEFAULT NULL,
     IN p_partitions_required INTEGER DEFAULT 24,
     IN P_execute_date date default current_date
     )
RETURNS INTEGER
AS
$BODY$
DECLARE
v_count INTEGER:=p_partitions_required;
v_tables_exist_fromnow INTEGER DEFAULT NULL;
v_date TIMESTAMP(0) WITHOUT TIME ZONE;
v_max_end_data  TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT NULL;
v_to_date TIMESTAMP(0) WITHOUT TIME ZONE;
v_month INTEGER;
v_year  INTEGER;
v_table_name VARCHAR:=p_table_to_execute;
v_schema VARCHAR:=p_schema;
v_partition_table_name VARCHAR;
v_partition_table  VARCHAR;
v_tables_to_be_created INTEGER:=0;
v_interval VARCHAR;
v_interval_value VARCHAR;
v_interval_condition VARCHAR;
v_sql VARCHAR;
v_logic VARCHAR;
V_SUB INTEGER :=0;

BEGIN


SELECT schema_name,interval_value,interval_condition
  INTO v_schema,v_interval_value,v_interval_condition
  FROM aws_utility.master_partition_info 
 WHERE schema_name=p_schema 
   AND table_name=p_table_to_execute;


IF v_interval_value='3' AND v_interval_condition='MONTH' THEN

   v_interval_condition:='quarter';

END IF; 


     SELECT  CASE WHEN count(end_data)=0 THEN NULL ELSE count(end_data) END,max(end_data) 
       INTO  STRICT v_tables_exist_fromnow,v_max_end_data
       FROM  (SELECT  tablename,
            partition_name,
            expr,
            substr(expr,19,10)::DATE st_date,
            substr(expr,46,'10')::DATE end_data
       FROM  (SELECT  ptt.schemaname,
                    base_tb.relname As tablename ,
                    pt.relname as partition_name, 
                    pg_get_expr(pt.relpartbound, pt.oid, true) AS expr
               FROM pg_class base_tb 
               JOIN pg_inherits i on i.inhparent = base_tb.oid 
               JOIN pg_class pt on pt.oid = i.inhrelid
               JOIN pg_tables ptt on base_tb.relname=ptt.tablename
              WHERE base_tb.relname=p_table_to_execute
                AND ptt.schemaname=p_schema
                AND pg_get_expr(pt.relpartbound, pt.oid, true)<>'DEFAULT'
                AND pg_get_expr(pt.relpartbound, pt.oid, true)  NOT LIKE '%MINVALUE%')B) a 
              WHERE st_date>=date_trunc(v_interval_condition,P_execute_date);

      
    IF (v_interval_condition='YEAR' OR  v_interval_condition='quarter') AND date_trunc(v_interval_condition,P_execute_date)<date_trunc(v_interval_condition,CURRENT_DATE) THEN

       V_SUB:= 1;
       
    END IF; 

IF v_interval_value='3' AND v_interval_condition='quarter' THEN

   v_interval_condition:='MONTH';

END IF; 

      v_date:=date_trunc(v_interval_condition,v_max_end_data)::DATE;

      
   v_sql := 'SELECT  '''||v_max_end_data||'''::date + ( interval '''||v_interval_value||' ' || v_interval_condition ||''');' ;
 
   
   EXECUTE v_sql INTO  v_to_date;
   
   v_to_date:=date_trunc(v_interval_condition,v_to_date)::DATE;
   v_tables_to_be_created:=v_count-v_tables_exist_fromnow-V_SUB;
            
             IF v_tables_to_be_created>0 THEN
             
                   FOR i in 1..v_tables_to_be_created 
                   
                   loop
                        
                        IF  v_interval_condition='YEAR' THEN
                        v_month:=0; 
                    
                        ELSE
                        v_month:=EXTRACT(MONTH FROM  v_date);     
                        END IF; 
                      
                        v_year:=EXTRACT(YEAR FROM  v_date);
                      
                        v_partition_table_name:=v_table_name||'_'||v_month||'_'||v_year;
                                         
                        v_partition_table:='CREATE TABLE '||v_schema||'.'||v_partition_table_name||' PARTITION OF '||v_schema||'.'||v_table_name||' FOR VALUES FROM  ('''||v_date||''')'||' To ('''||v_to_date||''');';
                      
                        raise notice 'v_partition_table   %',v_partition_table;

                 --       EXECUTE v_partition_table;
                      
                        v_sql := 'SELECT  '''||v_date||'''::date + ( interval '''||v_interval_value||' ' || v_interval_condition ||''');' ;
                        EXECUTE v_sql INTO  v_date;
                                                    
                        v_sql := 'SELECT  '''||v_to_date||'''::date + ( interval '''||v_interval_value||' ' || v_interval_condition ||''');' ;
                        EXECUTE v_sql INTO  v_to_date;
                       
                        i := i+1 ;
                       
                   END LOOP;          
             
                  RETURN v_tables_to_be_created;
                  
             END IF; 
             
      RETURN v_tables_to_be_created;
     

    EXCEPTION
        WHEN no_data_found  THEN
      RAISE USING ERRCODE := '20001', MESSAGE := CONCAT_WS('','Current month partition does not exist for table ', p_table_to_execute);

END;
$BODY$
LANGUAGE  plpgsql;




