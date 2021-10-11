select 
    TABLE_CATALOG as table_id, 
    table_name as table_name
from 
    information_schema.tables 
where 
    TABLE_SCHEMA = 'mysql';