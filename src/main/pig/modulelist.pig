register 'akela-0.2-SNAPSHOT.jar'
register 'socorro-toolbox-0.1-SNAPSHOT.jar'

raw = LOAD 'hbase://crash_reports' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'processed_data:json') AS (k:chararray, processed_json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(processed_json) AS processed_json_map:map[];
product_filtered = FILTER genmap BY processed_json_map#'product' == 'Firefox' AND processed_json_map#'os_name' == 'Windows NT';
module_bag = FOREACH product_filtered GENERATE com.mozilla.socorro.pig.eval.ModuleBag(processed_json_map#'dump') AS modules:bag{module_tuple:tuple(module:chararray, libname:chararray, version:chararray, pdb:chararray, checksum:chararray, addr_start:chararray, addr_end:chararray, unknown:chararray)};
filtered_modules = FILTER module_bag BY modules is not null AND modules.module is not null AND modules.version is not null AND modules.pdb is not null;
flat_modules = FOREACH filtered_modules GENERATE FLATTEN(modules);
modules_list = FOREACH flat_modules GENERATE module, version, pdb;
STORE modules_list INTO '$start_date-$end_date-module-list' USING PigStorage(',');
