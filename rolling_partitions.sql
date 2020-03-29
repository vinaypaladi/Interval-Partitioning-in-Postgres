CREATE OR REPLACE FUNCTION aws_utility.rolling_partitions(
     IN p_schema_name varchar,
     IN p_partitions_required INTEGER DEFAULT 12
     )
RETURNS aws_utility.rolling_partitions_array[]
AS
$BODY$
DECLARE
ITB RECORD;
rollong_tables_status aws_utility.rolling_partitions_array[];
T  int:=1;
Tble_count INTEGER;
BEGIN

FOR ITB IN (
            SELECT schema_name,table_name,interval_value,interval_condition
              FROM aws_utility.master_partition_info 
             WHERE partitioning_type='range' 
               AND schema_name=p_schema_name
             )
LOOP          
            
            Tble_count := aws_utility.create_rolling_partitions(ITB.schema_name, ITB.table_name,p_partitions_required);
                                       
            rollong_tables_status[T]:=(ITB.table_name,Tble_count);
                    
            T:=T+1;                   
END LOOP;     

RETURN rollong_tables_status;              
                 

EXCEPTION
WHEN others THEN
RAISE;
END;
$BODY$
LANGUAGE  plpgsql;
