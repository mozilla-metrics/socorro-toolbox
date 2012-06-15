REGISTER 'akela-0.4-SNAPSHOT.jar'
REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile socorro-dumpsizetrends.log;
SET default_parallel 2;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE FormatDate com.mozilla.pig.eval.date.FormatDate('yyyy-MM-dd');
DEFINE BytesSize com.mozilla.pig.eval.BytesSize();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'meta_data:json,processed_data:json,raw_data:dump',
                                                                                   'true') AS 
                                                                                   (k:bytearray, 
                                                                                    meta_json:chararray, 
                                                                                    processed_json:chararray, 
                                                                                    raw_dump:bytearray);
gen_data = FOREACH raw GENERATE JsonMap(meta_json) AS meta_json_map:map[], meta_json, processed_json, raw_dump;
filtered_data = FILTER gen_data BY meta_json_map#'timestamp' IS NOT NULL;
sizes = FOREACH filtered_data GENERATE FormatDate((meta_json_map#'timestamp'*1000.0)) AS day,
                                       meta_json_map#'ProductName' AS product_name:chararray,
                                       meta_json_map#'Version' AS product_version:chararray,
                                       (meta_json_map#'ProductID' IS NULL OR meta_json_map#'ProductID' != '{aa3c5121-dab2-40e2-81ca-7ea25febc110}' ? 1 : 0) AS is_xul:int,
                                       BytesSize(raw_dump) AS raw_size:long, 
                                       BytesSize(meta_json) AS meta_size:long, 
                                       BytesSize(processed_json) AS processed_size:long;
filtered_sizes = FILTER sizes BY product_name == 'Firefox' OR 
                                 product_name == 'Fennec' OR 
                                 product_name == 'SeaMonkey' OR 
                                 product_name == 'Thunderbird';
STORE filtered_sizes INTO '$start_date-$end_date-dumpsizes' USING PigStorage();

grouped = GROUP filtered_sizes BY (day,product_name,product_version,is_xul);
daily_sums = FOREACH grouped GENERATE FLATTEN(group) AS (day,product_name,product_version,is_xul),
                                      COUNT(filtered_sizes) AS doc_count:long,
                                      AVG(filtered_sizes.raw_size) AS avg_raw_size:double,
                                      AVG(filtered_sizes.meta_size) AS avg_meta_size:double,
                                      AVG(filtered_sizes.processed_size) AS avg_processed_size:double,
                                      MAX(filtered_sizes.raw_size) AS max_raw_size:double;
STORE daily_sums INTO '$start_date-$end_date-agg-dumpsizes' USING PigStorage();
