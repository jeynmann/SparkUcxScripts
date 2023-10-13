class SparkLogFilter:
    def __init__(self):
        self.filters = {}

    def __call__(self, log_file):
        for f in self.filters.values():
            if (f(log_file)):
                return True
        return False

    def add(self, filter):
        self.filters[filter.name] = filter


class SparkLogAppidFilter:
    def __init__(self, appid):
        self.name = "SparkLogAppidFilter"
        self.appid = appid
        self.ignores = []
        self.allowed = []

    def ignore(self, id):
        self.ignores.add(f"application_{self.appid}_{id:0>4}")

    def allow(self, id):
        self.allowed.add(f"application_{self.appid}_{id:0>4}")

    def ignore_range(self, ignores):
        for i in ignores:
            self.ignore(i)

    def allow_range(self, allowed):
        for i in allowed:
            self.allow(i)

    def __call__(self, log_file):
        return (self.ignores and log_file in self.ignores) or (self.allowed and log_file not in self.allowed)
