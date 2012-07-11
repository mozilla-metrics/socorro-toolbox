REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'lib/akela-0.4-SNAPSHOT.jar'

SET pig.logfile socorro-sandbox.log;
SET default_parallel 2;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE ModuleBag com.mozilla.socorro.pig.eval.ModuleBag();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'meta_data:json,processed_data:json',
                                                                                   'true') AS 
                                                                                   (k:bytearray, meta_json:chararray, processed_json:chararray);
genmap = FOREACH raw GENERATE k,JsonMap(meta_json) AS meta_json_map:map[], JsonMap(processed_json) AS processed_json_map:map[];
fltrd = FILTER genmap BY meta_json_map#'FlashProcessDump' IS NOT NULL;
outputdata = FOREACH fltrd GENERATE k,
                                    meta_json_map#'FlashProcessDump' AS fpd:chararray,
                                    processed_json_map#'signature' AS sig:chararray;

STORE outputdata INTO 'flashprocessdumps-$start_date-$end_date' USING PigStorage();
