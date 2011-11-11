register 'akela-0.2-SNAPSHOT.jar'
register 'socorro-toolbox-0.1-SNAPSHOT.jar'

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('111110', '111110', 'yyMMdd', 'processed_data:json', 'true') AS (k:chararray, json:chararray); 
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered = FILTER genmap BY json_map#'signature' == 'js::Bindings::getLocalNameArray(JSContext*, js::Vector<JSAtom*, int, js::TempAllocPolicy>*)';
/*
tanzan_indices = FOREACH filtered GENERATE INDEXOF(json_map#'dump', 'RapportTanzan', 0);
STORE tanzan_indices INTO 'tanzans' USING PigStorage();
*/

modules = FOREACH filtered GENERATE FLATTEN(com.mozilla.socorro.pig.eval.ModuleBag(json_map#'dump')) AS module:chararray;
grouped = GROUP modules BY module;
counted = FOREACH grouped GENERATE FLATTEN(group),COUNT(modules);
STORE counted INTO 'tanzans-counts' USING PigStorage();