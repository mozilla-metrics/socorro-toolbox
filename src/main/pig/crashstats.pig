REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'lib/akela-0.4-SNAPSHOT.jar'

SET pig.logfile socorro-crashstats.log;
SET default_parallel 30; 
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE ParseDate com.mozilla.pig.eval.date.ParseDate('yyMMdd');
DEFINE ISODate com.mozilla.pig.eval.date.FormatDate('yyyy-MM-dd');
DEFINE Size com.mozilla.pig.eval.Size();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'meta_data:json,processed_data:json',
                                                                                   'true') AS 
                                                                                   (k:bytearray, meta_json:chararray, processed_json:chararray);
gen_meta_map = FOREACH raw GENERATE JsonMap(meta_json) AS meta_json_map:map[], processed_json;
product_filtered = FILTER gen_meta_map BY meta_json_map#'ProductName' == 'Firefox' AND
                                          meta_json_map#'CrashTime' IS NOT NULL AND
                                          Size(TRIM(meta_json_map#'CrashTime')) > 0 AND
                                          Size(meta_json_map#'CrashTime') <= 20;
product_data = FOREACH product_filtered GENERATE meta_json_map, processed_json,
                                                ISODate((long)meta_json_map#'CrashTime' * (long)1000) AS crash_time_date:chararray;
/* in seconds need to compare to start_date and stop_date but in the case of a single day you want to make sure
   that you check against the start and the end of the day*/
/* time_filtered = FILTER product_data BY (crash_time_millis >= ParseDate('$start_date') AND 
                                        crash_time_millis <= ParseDate('$end_date')); */

/* count and output submission */
submissions = FOREACH product_data GENERATE crash_time_date, 
                                            meta_json_map#'ProductName' AS product_name:chararray, 
                                            meta_json_map#'Version' AS version:chararray, 
                                            'submissions' AS type:chararray;
grpd_submissions = GROUP submissions BY (crash_time_date,product_name,version,type);
submissions_counts = FOREACH grpd_submissions GENERATE FLATTEN(group), COUNT(submissions) AS cnt:long;
STORE submissions_counts INTO '$start_date-$end_date-submissions';

/* count and output hang */
hang_filtered = FILTER product_data BY meta_json_map#'HangId' IS NOT NULL;
hangs = FOREACH hang_filtered GENERATE crash_time_date, 
                                       meta_json_map#'ProductName' AS product_name:chararray, 
                                       meta_json_map#'Version' AS version:chararray, 
                                       'hangs' AS type:chararray;
grpd_hangs = GROUP hangs BY (crash_time_date,product_name,version,type);
hang_counts = FOREACH grpd_hangs GENERATE FLATTEN(group), COUNT(hangs) AS cnt:long;
STORE hang_counts INTO '$start_date-$end_date-hangs';

/* count and output oopp */
oopp_filtered = FILTER product_data BY meta_json_map#'ProcessType' IS NOT NULL AND meta_json_map#'ProcessType' == 'plugin';
oopps = FOREACH oopp_filtered GENERATE crash_time_date, 
                                       meta_json_map#'ProductName' AS product_name:chararray, 
                                       meta_json_map#'Version' AS version:chararray, 
                                       'oopp' AS type:chararray;
grpd_oopps = GROUP oopps BY (crash_time_date,product_name,version,type);
oopps_counts = FOREACH grpd_oopps GENERATE FLATTEN(group), COUNT(oopps) AS cnt:long;
STORE oopps_counts INTO '$start_date-$end_date-oopps';

/* count and output processed */
processed_filtered = FILTER product_data BY processed_json IS NOT NULL;
processed = FOREACH processed_filtered GENERATE crash_time_date, 
                                                meta_json_map#'ProductName' AS product_name:chararray, 
                                                meta_json_map#'Version' AS version:chararray, 
                                                'processed' AS type:chararray;
grpd_processed = GROUP processed BY (crash_time_date,product_name,version,type);
processed_counts = FOREACH grpd_processed GENERATE FLATTEN(group), COUNT(processed) AS cnt:long;
STORE processed_counts INTO '$start_date-$end_date-processed';
