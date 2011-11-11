register 'akela-0.2-SNAPSHOT.jar'

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'meta_data:json,processed_data:json,raw_data:dump') AS (meta_json:chararray,processed_json:chararray,raw_dump:bytearray);
gen_data = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(meta_json) AS meta_json_map:map[], meta_json, processed_json, raw_dump;
filtered_data = FILTER gen_data BY meta_json_map#'timestamp' IS NOT NULL;
sizes = FOREACH filtered_data GENERATE com.mozilla.pig.eval.date.FormatDate('yyyy-MM-dd', (meta_json_map#'timestamp'*1000)) AS day, com.mozilla.pig.eval.BytesSize(raw_dump) AS raw_size:long, com.mozilla.pig.eval.BytesSize(meta_json) AS meta_size:long, com.mozilla.pig.eval.BytesSize(processed_json) AS processed_size:long;
grouped = GROUP sizes BY day;
daily_sums = FOREACH grouped GENERATE group AS day,SUM(sizes.raw_size),SUM(sizes.meta_size),SUM(sizes.processed_size);
STORE daily_sums INTO '$start_date-$end_date-dumpsizes' USING PigStorage();
