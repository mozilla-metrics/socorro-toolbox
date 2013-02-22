REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar';
REGISTER 'akela-0.4-SNAPSHOT.jar';

SET pig.logfile LookupATIFrames.log;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE LookupATIFrames com.mozilla.socorro.pig.eval.LookupATIFrames();

raw = LOAD 'hbase://crash_reports'
    USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date',
      'yyMMdd', 'processed_data:json', 'true')
    AS (k:bytearray, processed_json:chararray);

processed = FILTER raw BY processed_json IS NOT NULL;

genmap = FOREACH processed GENERATE
    SUBSTRING(k, 7, 43) AS uuid,
    JsonMap(processed_json) AS processed_json_map:map[];

limited = FILTER genmap BY
    processed_json_map#'product' == 'Firefox'
    AND processed_json_map#'version' == '19.0'
    AND processed_json_map#'signature' == 'XPC_WN_Helper_NewResolve';


categorized = FOREACH limited GENERATE
    uuid,
    LookupATIFrames(processed_json_map#'dump') AS atiframe;

grouped = GROUP categorized BY atiframe;

totals = FOREACH grouped GENERATE
    group,
    COUNT(categorized) AS c;

DUMP totals;

allexamples = FILTER categorized BY
    atiframe == 2;

examples = LIMIT allexamples 50;

STORE examples INTO 'LookupATIFrames-examples-$start_date-$end_date' USING PigStorage();
