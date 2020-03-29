----type to display the output of rolling_partitions

CREATE TYPE aws_utility.rolling_partitions_array AS
(
table_name VARCHAR,
table_count INTEGER
);
