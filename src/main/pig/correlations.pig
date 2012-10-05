REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'akela-0.4-SNAPSHOT.jar'

SET pig.logfile socorro-correlations.log;
SET default_parallel 30; 
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE ModuleBag com.mozilla.socorro.pig.eval.ModuleBag();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'processed_data:json',
                                                                                   'true') AS 
                                                                                   (k:bytearray, processed_json:chararray);
genmap = FOREACH raw GENERATE JsonMap(processed_json) AS processed_json_map:map[];
modules = FOREACH genmap GENERATE processed_json_map#'product' AS product:bytearray,
                                  processed_json_map#'version' AS version:bytearray,
                                  processed_json_map#'os_name' AS os_name:bytearray,
                                  processed_json_map#'reason' AS reason:bytearray,
                                  FLATTEN(ModuleBag(processed_json_map#'dump')) AS
                                          (filename:chararray, module_version:chararray,
                                           debug_file:chararray, debug_id:chararray, base_addr:chararray,
                                           max_addr:chararray, is_main_module:chararray);
ss = FOREACH modules GENERATE filename,module_version,debug_file,debug_id,product,version,os_name,reason;
/* Ask pig mailing list why this works but DISTINCT ss; doesn't */
grpd = GROUP ss BY (filename,debug_file,debug_id,module_version,product,version,os_name,reason);
distinct_modules = FOREACH grpd GENERATE FLATTEN(group), COUNT(ss);

STORE distinct_modules INTO 'correlations-$start_date-$end_date' USING PigStorage(',');
