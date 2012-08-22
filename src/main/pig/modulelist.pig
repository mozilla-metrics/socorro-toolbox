REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'akela-0.4-SNAPSHOT.jar'

SET pig.logfile socorro-modulelist.log;
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
product_filtered = FILTER genmap BY processed_json_map#'product' == 'Firefox' AND 
                                    processed_json_map#'os_name' == 'Windows NT';
modules = FOREACH product_filtered GENERATE FLATTEN(ModuleBag(processed_json_map#'dump')) AS
                                            (filename:chararray, version:chararray,
                                            debug_file:chararray, debug_id:chararray, base_addr:chararray,
                                            max_addr:chararray, is_main_module:chararray);
fltrd = FILTER modules BY filename matches '.*\\.dll$' AND
                          (version matches '\\d+\\.\\d+\\.\\d+\\.\\d+' OR version == '') AND
                          (debug_file matches '.*\\.pdb$' OR debug_file == '') AND
                          (SIZE(debug_id) == 33 OR debug_id == '');
ss = FOREACH fltrd GENERATE filename,version,debug_file,debug_id;
/* Ask pig mailing list why this works but DISTINCT ss; doesn't */
grpd = GROUP ss BY (filename,debug_file,debug_id,version);
distinct_modules = FOREACH grpd GENERATE FLATTEN(group);

STORE distinct_modules INTO 'modulelist-$start_date-$end_date' USING PigStorage(',');
