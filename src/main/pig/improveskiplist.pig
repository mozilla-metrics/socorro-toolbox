REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar'
REGISTER 'lib/akela-0.4-SNAPSHOT.jar'
REGISTER 'pig_socorro.py' using jython as pigsocorro;

SET pig.logfile improveskiplist.log;
SET default_parallel 2;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

DEFINE Example com.mozilla.pig.eval.Example();
DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE SignatureForDump pigsocorro.signature_for_dump();

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 
                                                                                   'yyMMdd',
                                                                                   'meta_data:json,processed_data:json',
                                                                                   'true') AS 
                                                                                   (k:bytearray, meta_json:chararray, processed_json:chararray);

processed = FILTER raw BY processed_json IS NOT NULL;

genmap = FOREACH processed GENERATE
  SUBSTRING(k, 7, 43) AS uuid,
  JsonMap(meta_json) AS meta_json_map:map[],
  JsonMap(processed_json) AS processed_json_map:map[];

filterjava = FILTER genmap BY meta_json_map#'JavaStackTrace' IS NULL;

signatures = FOREACH filterjava GENERATE
  uuid,
  processed_json_map#'signature' AS signature,
  SignatureForDump(processed_json_map, meta_json_map#'Hang') AS newsignature;

unmatched = FILTER signatures BY signature != newsignature;

tuplegroup = GROUP unmatched BY (signature, newsignature);

grouped = FOREACH tuplegroup GENERATE
  group.signature,
  group.newsignature,
  COUNT(unmatched) AS c,
  Example(unmatched.uuid).$0 as uuid;

STORE grouped INTO 'improveskiplist-$start_date-$end_date' USING PigStorage();
