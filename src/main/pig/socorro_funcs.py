import os, sys

import org.apache.pig.tools.pigstats.PigStatusReporter as PigStatusReporter
import org.apache.pig.tools.counters.PigCounterHelper as PigCounterHelper
import org.apache.pig.impl.util.UDFContext as UDFContext


reporter = PigCounterHelper()

@outputSchema('modules:bag{t:tuple(filename:chararray,version:chararray,debug_file:chararray,debug_id:chararray,base_addr:chararray,max_addr:chararray)}')
def get_modules(strx):
    retval = []
    try:
        for i in strx[0]:
            retval.append((i['filename'], i['version'], i['debug_file'], i['debug_id'], i['base_addr'], i['end_addr']))
    except:
        reporter.incrCounter('stats', 'errors', 1)

    return retval
