from socorro.processor.signature_utilities import CSignatureTool
from socorro.lib.util import DotDict, emptyFilter
import config.processorconfig as old

config = DotDict()
config.irrelevant_signature_re = old.irrelevantSignatureRegEx.default
config.prefix_signature_re = old.prefixSignatureRegEx
config.signatures_with_line_numbers_re = old.signaturesWithLineNumbersRegEx.default
config.signature_sentinels = old.signatureSentinels.default
c_tool = CSignatureTool(config)

if __name__ == '__main__':
    def outputSchema(t):
        return lambda(f): f

@outputSchema("signature:chararray")
def signature_for_dump(processed_json, chromehang):
    if processed_json is None:
        return None

    dump = processed_json.get('dump', None)
    if dump is None:
        return None

    if chromehang:
        hang = 1
    elif processed_json.get('hangid', None) is not None:
        hang = -1
    else:
        hang = 0

    if processed_json.get('os_name', None) == "Windows NT":
        makelowercase = True
    else:
        makelowercase = False

    crashedThread = processed_json.get('crashedThread', None)
    if hang == 1:
        crashedThread = 0

    signatureList = []
    for line in dump.splitlines():
        items = map(emptyFilter, line.split('|'))
        if len(items) != 7:
            continue
        thread_num, frame_num, module_name, function, source, source_line, instruction = items
        if int(thread_num) == crashedThread:
            if int(frame_num) > 30:
                continue
            if makelowercase and module_name is not None:
                module_name = module_name.lower()
            signatureList.append(c_tool.Frame(module_name, function, source, source_line, instruction))

    return c_tool.generate(signatureList, hang, crashedThread)[0]

if __name__ == '__main__':
    import sys, json

    print signature_for_dump(json.load(sys.stdin), bool(sys.argv[1]))
