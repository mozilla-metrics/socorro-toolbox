register 'akela-0.2-SNAPSHOT.jar'
register 'socorro-toolbox-0.1-SNAPSHOT.jar'

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'processed_data:json') AS (k:chararray, processed_json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(processed_json) AS processed_json_map:map[];
product_filtered = FILTER genmap BY processed_json_map#'product' == 'Firefox' AND processed_json_map#'os_name' == 'Windows NT';
stack_bag = FOREACH product_filtered GENERATE k,com.mozilla.socorro.pig.eval.FrameBag(processed_json_map#'dump') AS frames:bag{frame:tuple(frame_group:chararray, frame_index:chararray, module:chararray, method:chararray, src_file:chararray, num:chararray, hex:chararray)};
flat_stack = FOREACH stack_bag GENERATE k,FLATTEN(frames);
STORE flat_stack INTO '$start_date-$end_date-stackframes' USING PigStorage();

method_sigs = FOREACH flat_stack GENERATE $5 AS (method:chararray);
grouped_sigs = GROUP method_sigs BY method;
distinct_sigs = FOREACH grouped_sigs GENERATE group, COUNT(method_sigs);
STORE distinct_sigs INTO '$start_date-$end_date-method-signatures' USING PigStorage();
