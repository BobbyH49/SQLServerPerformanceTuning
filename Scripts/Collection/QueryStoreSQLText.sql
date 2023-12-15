select q.query_id, t.query_text_id, t.query_sql_text
from sys.query_store_query_text t
join sys.query_store_query q on q.query_text_id = t.query_text_id
order by query_id asc
