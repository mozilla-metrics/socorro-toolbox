REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar';
REGISTER 'akela-0.4-SNAPSHOT.jar';

SET pig.logfile MissingSymbols.log;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE MissingSymbols com.mozilla.socorro.pig.eval.MissingSymbols();

raw = LOAD 'hbase://crash_reports'
    USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date',
      'yyMMdd', 'processed_data:json', 'true')
    AS (k:bytearray, processed_json:chararray);

processed = FILTER raw BY processed_json IS NOT NULL;

genmap = FOREACH processed GENERATE
    JsonMap(processed_json) as processed_data:map[];

limited = FILTER genmap BY
    processed_data#'os_name' == 'Windows NT';

functed = FOREACH limited GENERATE
    MissingSymbols(processed_data#'dump') AS modules;

flattened = FOREACH functed GENERATE
    flatten(modules);

grouped = GROUP flattened BY (pdbname, id);

totals = FOREACH grouped GENERATE
    group.pdbname,
    group.id,
    COUNT(flattened) AS c;

relevant = FILTER totals BY c > 2;

STORE relevant INTO 'MissingSymbols-windows-$start_date-$end_date'
    USING PigStorage();
