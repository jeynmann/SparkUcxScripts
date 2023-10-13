import os
import csv
import sys


class BatchGroupByTest:
    def __init__(self) -> None:
        self.conf_dir = "."
        self.conf_file = "config_cx6_01.csv"
        self.cmd = "echo -e '\e[031mok\e[0m'"
    def __call__(self, *args, **kwds) -> None:
        conf_path = "{}/{}".format(self.conf_dir, self.conf_file)
        with open(conf_path) as conf:
            reader = csv.DictReader(conf)
            for row in reader:
                environ = ""
                for k,v in row.items():
                    environ = "{} {}={}".format(environ, k,v)
                command = "{} {}".format(environ, self.cmd)
                with os.popen(command) as output:
                    print(command)
                    for line in output.readlines():
                        print(line)

if __name__ == "__main__":
    test = BatchGroupByTest()
    if len(sys.argv) > 2:
        conf_dir = sys.argv[1]
        conf_file = sys.argv[2]
    elif len(sys.argv) > 1:
        conf_dir = "."
        conf_file = sys.argv[1]
    else:
        conf_dir = "."
        conf_file = "config_cx6_01.csv"
    test.conf_dir = conf_dir
    test.conf_file = conf_file
    test.cmd = "runtestrdma.sh"
    test()