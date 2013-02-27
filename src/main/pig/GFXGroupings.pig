REGISTER 'socorro-toolbox-0.1-SNAPSHOT.jar';
REGISTER 'akela-0.4-SNAPSHOT.jar';

SET pig.logfile LookupATIFrames.log;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE JsonMap com.mozilla.pig.eval.json.JsonMap();
DEFINE LookupATIFrames com.mozilla.socorro.pig.eval.LookupATIFrames();
DEFINE NearTopOfStack com.mozilla.socorro.pig.eval.NearTopOfStack();

raw = LOAD 'hbase://crash_reports'
    USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date',
      'yyMMdd', 'meta_data:json,processed_data:json', 'true')
    AS (k:bytearray, meta_json:chararray, processed_json:chararray);

processed = FILTER raw BY processed_json IS NOT NULL;

genmap = FOREACH processed GENERATE
    k,
    SUBSTRING(k, 7, 43) AS uuid,
    JsonMap(meta_json) AS meta_data:map[],
    JsonMap(processed_json) AS processed_data:map[];

limited = FILTER genmap BY
    processed_data#'os_name' == 'Windows NT' AND
    processed_data#'product' == 'Firefox' AND
    processed_data#'version' == '19.0' AND (
      processed_data#'signature' == 'nsXPConnect::GetXPConnect()' OR
      processed_data#'signature' == 'TlsGetValue' OR
      processed_data#'signature' == 'InterlockedIncrement'
    );

categorized = FOREACH limited GENERATE
    k,
    uuid,
    LookupATIFrames(processed_data#'dump') AS atiframe,
    (chararray) meta_data#'AdapterVendorID' as AdapterVendorID,
    REGEX_EXTRACT(processed_data#'app_notes', 'AdapterDriverVersion: ([\\d.]+)', 1) AS AdapterDriverVersion;

bydriver = GROUP categorized BY (AdapterVendorID, AdapterDriverVersion);

DESCRIBE bydriver;

drivertotals = FOREACH bydriver GENERATE
    group.AdapterVendorID,
    group.AdapterDriverVersion,
    COUNT(categorized) AS c;

STORE drivertotals INTO 'GFXGroupings-bydriver-$start_date-$end_date'
    USING PigStorage();

allrunning = FILTER categorized BY
    atiframe == 2;

running = LIMIT allrunning 50;

STORE running INTO 'LookupATIFrames-running-$start_date-$end_date' USING PigStorage();
