class ConfigDict(dict):
    __slots__ = ()

    def __init__(self, __val__=(), subconfigs=None, **kwargs):
        dict.__init__(self, __val__)
        if subconfigs is not None:
            for path in subconfigs:
                parts = path.split('.')
                c = self
                for part in parts:
                    c = c.setdefault(part, ConfigDict())

        for k, v in kwargs.iteritems():
            self[k] = v

    def __getattr__(self, key):
        return self.get(key, None)

    def __setattr__(self, key, value):
        self[key] = value;
