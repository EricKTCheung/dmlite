#!/usr/bin/env python
from __future__ import print_function, division, absolute_import

import gfal2
import StringIO
import traceback
import datetime
import time
import threading
import argparse
import errno
import inspect
import os
import filecmp
import hashlib
import urllib
import signal
import stat
import sys
import re
import textwrap
import json
import subprocess
import random
from urlparse import urlparse

EX_OK              = 0
EX_WARNING         = 1
EX_CRITICAL        = 2
EX_UNKNOWN         = 3
EX_SKIPPED         = 4

DOME_MAX_RESPONSE_SIZE = 1024 * 1024 * 5
TEST_TIMEOUT = 10
TEST_CLEANUP = True
TEST_VERBOSE = False
TEST_IGNORE_ERRORS = False
USE_COLORS = True
USE_XML = False

class Color(object):
    sequences = {
        "red"      : "\033[91;1m",
        "green"    : "\033[92;1m",
        "yellow"   : "\033[93;1m",
        "blue"     : "\033[94;1m",
        "purple"   : "\033[95;1m",
        "cyan"     : "\033[96;1m",
        "gray"     : "\033[97;1m",

        "end"      : "\033[0m"
    }

    @staticmethod
    def colorize(c, s):
        if not USE_COLORS:
            return s
        return Color.sequences[c]+s+Color.sequences["end"]

    @staticmethod
    def red(s)   : return Color.colorize("red", s)

    @staticmethod
    def green(s) : return Color.colorize("green", s)

    @staticmethod
    def yellow(s) : return Color.colorize("yellow", s)

    @staticmethod
    def purple(s): return Color.colorize("purple", s)

    @staticmethod
    def cyan(s): return Color.colorize("cyan", s)

    @staticmethod
    def blue(s): return Color.colorize("blue", s)

    @staticmethod
    def gray(s): return Color.colorize("gray", s)

CANCEL = False
IMPATIENCE = 0
def ctrlc(signal, frame):
    global IMPATIENCE
    IMPATIENCE += 1

    global CANCEL
    CANCEL = True

    if IMPATIENCE == 1:
        print(Color.red("\nReceived ctrl-c, will halt after cleaning up testfiles .."))
    elif IMPATIENCE < 5:
        print(Color.red("\nhit ctrl-c {0} more times to terminate forcefully".format(5 - IMPATIENCE)))
    elif IMPATIENCE == 5:
        print(Color.red("\nterminating forcefully".format(5 - IMPATIENCE)))
        sys.exit(1)

def printable_outcome(outcome):
    if outcome == EX_OK:
        return "PASS"
    if outcome == EX_WARNING:
        return "WARNING"
    if outcome == EX_CRITICAL:
        return "FAIL"
    if outcome == EX_UNKNOWN:
        return "UNKNOWN"
    if outcome == EX_SKIPPED:
        return "SKIPPED"
    raise ValueError("invalid argument")

def colored_outcome(outcome):
    pt = printable_outcome(outcome)

    if outcome == EX_OK:
        return Color.green(pt)
    if outcome == EX_WARNING:
        return Color.yellow(pt)
    if outcome == EX_CRITICAL:
        return Color.red(pt)
    if outcome == EX_UNKNOWN:
        return Color.purple(pt)
    if outcome == EX_SKIPPED:
        return Color.purple(pt)
    raise ValueError("invalid argument")

def duration_in_sec(start, end):
    delta = end - start
    return delta.seconds + (delta.microseconds / 1000000)

def colorize_prefix(s):
    if s == "davs":
        return Color.purple(s)
    if s == "root":
        return Color.blue(s)
    if s == "gsiftp":
        return Color.cyan(s)
    if s == "srm":
        return Color.gray(s)
    return Color.gray(s)

def count_printable_chars(text):
    return len(re.sub(r'\033\[[\d;]*m', '', text))

def determine_terminal_width():
    try:
        return int(os.popen('stty size', 'r').read().split()[1])
    except:
        return -1

def terminal_align(left, right):
    width = determine_terminal_width()
    chars_left = count_printable_chars(left)
    chars_right = count_printable_chars(right)
    space = width - chars_left - chars_right

    if space < 0: space = 1

    return left + space * " " + right

TEST_NUMBER = 0
def get_test_number():
    global TEST_NUMBER
    TEST_NUMBER += 1
    return TEST_NUMBER


class TestResult:
    """A class to store and display the results from a single test run"""
    def __init__(self, prefix, name):
        self.prefix = prefix
        self.name = name
        self.outcome = EX_UNKNOWN
        self.output = []
        self.starttime = datetime.datetime.now()

    def reset_name(self, prefix, name):
        self.prefix = prefix
        self.name = name

    def success(self, msg=None):
        self.endtime = datetime.datetime.now()
        self.outcome = EX_OK
        self.write(msg)
        return self

    def failure(self, msg=None):
        self.endtime = datetime.datetime.now()
        self.outcome = EX_CRITICAL
        self.write(msg)
        return self

    def absorb(self, result, status_change=True):
        if status_change and not result.ok():
            self.outcome = EX_CRITICAL
            self.endtime = result.endtime

        self.output.append(result)

        # did I survive after absorbing another test result?
        return self.alive()

    def write(self, s):
        if s:
            self.output.append(str(s))

    def setduration(self, d):
        self.duration = d

    def ok(self):
        return self.outcome == EX_OK

    def alive(self):
        return self.outcome == EX_UNKNOWN or self.outcome == EX_OK

    @staticmethod
    def show_pending(prefix, name, indent=""):
        if USE_XML: return
        print(indent + ".... => {0} :: {1}".format(colorize_prefix(prefix), name), end="\r")
        sys.stdout.flush()

    def get_status_line(self, indent=""):
        result = colored_outcome(self.outcome)
        duration = duration_in_sec(self.starttime, self.endtime)
        return terminal_align(indent + "{0} => {1} :: {2}".format(result, colorize_prefix(self.prefix), self.name),
                           "{0:.5f} sec".format(duration))

    def get_output(self, indent="    "):
        strio = StringIO.StringIO()
        for item in self.output:
            if type(item) is str:
                for line in textwrap.wrap(item, 120, subsequent_indent=indent, initial_indent=indent):
                    strio.write(line)
                    strio.write("\n")
            else:
                strio.write(item.get_status_line(indent))
                strio.write("\n")
                strio.write(item.get_output(indent + "    "))
        return strio.getvalue()

    def show_xml(self):
        print("""<testcase name="{0:02d} {1}" classname="{2}" time="{3}">""".format(get_test_number(), urllib.quote(self.name), self.prefix,
              duration_in_sec(self.starttime, self.endtime)))

        if not self.ok():
            print("""<failure></failure>""")

        print("""<system-out><![CDATA[{0}]]></system-out></testcase>""".format(self.get_output()))

    def show(self):
        if USE_XML:
            self.show_xml()
            return

        print(self.get_status_line())
        if not self.ok() or TEST_VERBOSE:
            print(self.get_output(), end="")

# Executes an arbitrary function in a separate thread.
# Stores its output and any exception that might have occured
class ThreadExc(threading.Thread):
    def __init__(self, function, arguments):
        super(ThreadExc, self).__init__()

        self.function = function
        self.arguments = arguments

        self.output = None
        self.exception = None
        self.traceback = None

    def run(self):
        try:
            self.output = self.function(*self.arguments)
        except Exception as e:
            self.traceback = traceback.format_exc()
            self.exception = e

# Wraps the result from the execution of a gfal function
class GfalStatus:
    def __init__(self, output, exception, traceback, timed_out):
        self.output = output
        self.traceback = traceback
        self.exception = exception
        self.timed_out = timed_out

def raw_run_gfal(function, arguments):
    thread = ThreadExc(function, arguments)
    thread.daemon = True
    thread.start()
    thread.join(TEST_TIMEOUT)

    return GfalStatus(thread.output, thread.exception, thread.traceback,
                      thread.is_alive())

def check_timeout(status, result):
    if status.timed_out:
        result.failure("Operation timed out\n")
        return False
    return True

def expect_ok(status, result):
    if not check_timeout(status, result): return False

    if status.exception:
        result.failure(status.traceback)
        return False
    return True

def expect_exception(status, result, extype=None, excode=None):
    if not check_timeout(status, result): return False

    if not status.exception:
        result.failure("Error - expected to receive an exception\n")
        return False

    if extype and not isinstance(status.exception, extype):
        result.failure("Error - expected exception of type '{0}', received '{1}'\n".format(
                        extype.__name__, status.exception.__class__.__name__))
        return False

    if excode and not hasattr(status.exception, "code"):
        result.failure("Error - expected an exception with code {0}, but exception object has no code\n".format(excode))
        return False

    if excode and status.exception.code != excode:
        result.failure("Error - expected an exception with code {0}, received {1} instead\n".format(excode, status.exception.code))
        return False

    return True


# build a TestResult testname from a gfal function
def make_gfal_result(function, arguments):
    name = function.__name__

    result = TestResult("gfal", function.__name__)
    result.write(arguments)
    return result

def run_gfal_expect_ok(function, arguments):
    result = make_gfal_result(function, arguments)
    result.status = raw_run_gfal(function, arguments)
    if not expect_ok(result.status, result): return result
    return result.success()

def run_gfal_expect_exception(function, arguments, extype=None, excode=None):
    result = make_gfal_result(function, arguments)
    result.status = raw_run_gfal(function, arguments)
    if not expect_exception(result.status, result, extype=extype, excode=excode): return result
    return result.success()

def run_expect_ok(result, function, args):
    status = run_gfal(function, args)
    if expect_ok(status, result):
        result.success()
    return result

def run_expect_exception(result, function, args, extype=None, excode=None):
    status = run_gfal(function, args)
    if expect_exception(status, result, extype, excode):
        result.success()
    return result

def get_caller_name():
    caller_function_name = inspect.stack()[2][3]
    return caller_function_name.replace("_", " ")

def create_random_file(location, nbytes):
    with open(location, "wb") as f:
        with open("/dev/urandom", "rb") as f2:
            f.write(f2.read(nbytes))

# returns the filenames and checksums
def create_random_files(location_prefix, sizes):
    locations = []
    checksums = []
    for i in range(0, len(sizes)):
        locations.append(f("{location_prefix}{i}"))
        create_random_file(locations[-1], sizes[i])
        checksums.append(calculate_checksum("md5", locations[-1]))
    return (locations, checksums)

# Poor man's python 3.6 f-strings
# Uses local variables to format a string, ie
# b = 1, c = 2
# f("{b} {c}") => "1 2"
# When in 15 years python 3.6 becomes the minimum version to support,
# all invocations f("something") could be replaced with f"something"
def f(s):
    return s.format(**inspect.currentframe().f_back.f_locals)

class DomeCredentials(object):
    """Stores all credentials needed for a dome request"""
    def __init__(self, clientDN, clientAddress):
        self.clientDN = clientDN
        self.clientAddress = clientAddress

class DomeTalker:
    """Issues requests to Dome"""

    @staticmethod
    def build_url(url, command):
        while url.endswith("/"):
            url = url[:-1]
        return "{0}/command/{1}".format(url, command)

    def __init__(self, creds, uri):
        self.creds = creds
        self.uri = uri

    def execute(self, verb, domecmd, data):
        result = TestResult("DomeTalker", f("Issuing: {verb} {domecmd}"))

        cmd = ["davix-http"]
        if self.creds.clientDN:
            cmd += ["--header", "remoteclientdn: {0}".format(self.creds.clientDN)]
        if self.creds.clientAddress:
            cmd += ["--header", "remoteclientaddr: {0}".format(self.creds.clientAddress)]
        cmd += ["-P grid"]

        cmd += ["--data", json.dumps(data)]
        cmd += ["--request", verb]

        cmd.append(DomeTalker.build_url(self.uri, domecmd))

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result.status = proc.communicate()

        if proc.returncode != 0 or "(Davix::HttpRequest)" in result.status[1] or "Error" in result.status[1]:
            result.failure()
        else:
            result.success()

        result.write(f("Request body: {data}"))
        result.write(f("Response - stdout: {result.status[0]}"))
        result.write(f("Response - stderr: {result.status[1]}"))
        result.write(f("Davix exit code: {proc.returncode}"))
        return result

class DomeTester:
    def __init__(self, uri):
        self.uri = uri
        self.ctx = gfal2.creat_context()
        self.talker = DomeTalker(DomeCredentials(None, None), uri)

    def new_result(self, desc=None):
        result = TestResult("dome", get_caller_name())
        if desc: result.write(desc)
        return result

    def command_url(self, s):
        return self.uri + "command/" + s

    def setquotatoken(self, description, path, poolname, space):
        result = TestResult("dome", f("New quotatoken at {path} for {space} bytes"))

        res = self.talker.execute("POST", "dome_setquotatoken",
                                      { "description" : description,
                                        "path" : path,
                                        "poolname" : poolname,
                                        "quotaspace" : space})
        if result.absorb(res): return result.success()
        return result

    def delquotatoken(self, path, poolname):
        result = TestResult("dome", f("Delete quotatoken at {path} (pool: {poolname})"))

        res = self.talker.execute("POST", "dome_delquotatoken",
                                          { "path" : path, "poolname" : poolname})

        if not result.absorb(res): return result
        return result.success()

    def checksum(self, lfn, chtype, expected, nchecks, check_interval):
        result = TestResult("dome", f("Calculate {chtype} checksum on {lfn} through dome"))

        for i in range(0, nchecks):
            res = self.talker.execute("GET", "dome_chksum",
                                      { "lfn" : lfn,
                                        "checksum-type" : chtype})

            if not result.absorb(res): return result
            response = json.loads(res.status[0])

            if "checksum" in response:
                resp = response["checksum"]
                if resp == expected:
                    return result.success(f("Checksum verified: {expected}"))
                return result.failure(f('Error: expected checksum {expected}, received {resp}'))

            if i != nchecks - 1:
                result.write(f("Result not available yet, trying again in {check_interval} seconds.."))
                time.sleep(check_interval)

        return result.failure("Checksum not found.")

    def info(self):
        result = TestResult("dome", "Call dome_info, test if we have access")

        res = self.talker.execute("GET", "dome_info", {})
        if not result.absorb(res): return result

        if "ACCESS TO DOME GRANTED" in res.status[0]:
            return result.success()
        return result.failure()

    def parallel_checksums(self, lfns, expected, nchecks, check_interval):
        nfiles = len(lfns)
        assert nfiles == len(expected)
        result = TestResult("dome", f("Verify {nfiles} checksums in parallel."))

        functions = [self.checksum] * nfiles
        arguments = []
        for (lfn, exp) in zip(lfns, expected):
            arguments.append([extract_path(lfn), "md5", exp, nchecks, check_interval])

        return hammer_tester(functions, arguments)

def ensure_safe_path(path):
    if "dpm-test" not in path and "dpmtest" not in path:
        print("Internal error: attempted to touch a filename which contains neither 'dpm-test' nor 'dpmtest'. "
        "This ia a bug in the tests and should never have happened")
        sys.exit(1)

# assumption: second will always contain a single chunk, ie don't pass
# "/dir1/" along with "/dir2/file"
def path_join(first, second):
    second = urllib.quote(second)

    if first.endswith("/"):
        return first + second
    return "{0}/{1}".format(first, second)

def hammer_tester(functions, arguments):
    assert len(functions) == len(arguments)
    result = TestResult("HammerTester", "{0} functions in parallel".format(len(functions)))

    threads = []
    for (function, args) in zip(functions, arguments):
        thread = ThreadExc(function, args)
        thread.daemon = True
        thread.start()

        threads.append(thread)

    for thread in threads:
        # should never block indefinitely, since the individual tests will
        # eventually timeout
        thread.join()

        if thread.output:
            result.absorb(thread.output)
        else:
            result.write(thread.output)
            result.write(thread.exception)
            result.write(thread.traceback)
            result.failure("No output")

    if result.alive(): result.success()
    return result

class ProtocolTester:
    def __init__(self, testcase):
        self.ctx = gfal2.creat_context()

    def new_result(self, desc=None):
        result = TestResult("Tester", get_caller_name())
        if desc: result.write(desc)
        return result

    # wrap a gfal result
    def forward(self, child):
        result = TestResult("Tester", get_caller_name())
        result.absorb(child)
        result.outcome = child.outcome
        result.starttime = child.starttime
        result.endtime = child.endtime
        return result

    def Verify_base_directory_exists(self, directory):
        return self.forward(run_gfal_expect_ok(self.ctx.access, [directory, 0]))

    def Verify_directory_does_not_exist(self, directory):
        return run_gfal_expect_exception(self.ctx.access, [directory, 0],
                                         gfal2.GError, errno.ENOENT)

    def Create_directory(self, directory):
        return self.forward(run_gfal_expect_ok(self.ctx.mkdir, [directory, 0]))

    def Upload_testfile(self, source, destination, spacetoken=None, expect_failure=False):
        params = self.ctx.transfer_parameters()
        if spacetoken:
            params.dst_spacetoken = spacetoken

        if expect_failure:
            return self.forward(run_gfal_expect_exception(self.ctx.filecopy, [params, source, destination]))

        return self.forward(run_gfal_expect_ok(self.ctx.filecopy, [params, source, destination]))

    def Download_testfile(self, source, destination):
        self.ctx = gfal2.creat_context() # prevents client-side caching in gridftp..
        params = self.ctx.transfer_parameters()
        params.overwrite = True
        return self.forward(run_gfal_expect_ok(self.ctx.filecopy, [params, source, destination]))

    def Verify_downloaded_contents_are_the_same(self, original, downloaded):
        result = self.new_result(f("Compare the contents of '{original}' and '{downloaded}' for equality"))
        if not filecmp.cmp(original, downloaded):
            return result.failure("The two files differ!")

        return result.success()

    def Verify_checksum(self, target, checksumtype, checksum):
        result = self.new_result(f("Verify the {checksumtype} checksum of '{target}' is '{checksum}'"))
        res = run_gfal_expect_ok(self.ctx.checksum, [target, checksumtype])
        if not result.absorb(res): return result

        if checksum != res.status.output:
            return result.failure(f("Checksum mismatch! Expected '{checksum}', received '{res.status.output}'"))
        return result.success()

    def Verify_listing(self, target, listing, totalsize):
        result = self.new_result("")
        result.write("Verify listing of '{0}' is '{1}' with total size '{2}'".format(target, listing, totalsize))

        res = run_gfal_expect_ok(self.ctx.opendir, [target])
        if not result.absorb(res): return result
        dirp = res.status.output

        currentsize = 0
        while True:
            res = run_gfal_expect_ok(dirp.readpp, [])
            if not result.absorb(res): return result
            (dirent, fstat) = res.status.output
            if not dirent: break

            if dirent.d_name not in listing:
                return result.failure("Unexpected additional entry in listing: '{0}'".format(dirent.d_name))
            listing.remove(dirent.d_name)
            currentsize += fstat.st_size

        if listing != []:
            return result.failure("The following files were not found in listing: {0}".format(listing))
        if totalsize != currentsize:
            return result.failure("Filesize mismatch! Expected '{0}', received '{1}'".format(totalsize, currentsize))

        res = self.Verify_size(target, totalsize)
        if not result.absorb(res): return result

        return result.success()

    def Verify_size(self, target, size, directory=False, count=11):
        result = self.new_result("")
        result.write("Verify the size of '{0}' is '{1}'\n""".format(target, size))

        # gsiftp works in mysterious ways and globus callbacks,
        # which has a consequence that updates are delayed in some cases
        # Try again with an exponential backoff if size verification fails
        sleeptime = 0.05
        for i in range(0, count):
            res = run_gfal_expect_ok(self.ctx.stat, [target])
            if not result.absorb(res): return result

            if res.status.output.st_size == size:
                return result.success(f("Size ({size}) successfully verified."))

            result.write(f("Verify size failed, expected {size}, received {res.status.output.st_size}."))
            if i != count-1:
                if directory:
                    result.write("Renaming directory to potentially refresh memcache contents..")

                    res = run_gfal_expect_ok(self.ctx.rename, [target, target + "-tmp"])
                    if not result.absorb(res): return result

                    res = run_gfal_expect_ok(self.ctx.rename, [target + "-tmp", target])
                    if not result.absorb(res): return result

                result.write(f("Trying again after sleeping {sleeptime} seconds"))
                time.sleep(sleeptime)
                sleeptime *= 2

        return result.failure(f("Could not verify size after {count} attempts"))

    def Verify_directory_space_accounting(self, source, destdir, count=10, spacetoken=None):
        desc = """Upload '{0}' to '{1}/##' {2} times, verify space accounting at each step""".format(source, destdir, count)

        st = self.ctx.stat(source)
        result = self.new_result(desc)

        for i in range(0, count):
            res = self.Upload_testfile(source, "{0}/{1}".format(destdir, i), spacetoken=spacetoken)
            if not result.absorb(res): return result

            res = self.Verify_size(destdir, (i+1)*st.st_size, directory=True)
            if not result.absorb(res): return result

        return result.success()

    def Remove_file(self, target):
        ensure_safe_path(target)
        return self.forward(run_gfal_expect_ok(self.ctx.unlink, [target]))

    def Remove_files_and_verify_space_accounting(self, target, totalsize):
        ensure_safe_path(target)
        desc = """Delete contents of '{0}', verify space accounting at each step""".format(target)
        result = self.new_result(desc)

        res = run_gfal_expect_ok(self.ctx.opendir, [target])
        if not result.absorb(res): return result
        dirp = res.status.output

        currentsize = totalsize
        while True:
            res = run_gfal_expect_ok(dirp.readpp, [])
            if not result.absorb(res): return result
            (dirent, fstat) = res.status.output
            if not dirent: break

            res = self.Remove_file(path_join(target, dirent.d_name))
            if not result.absorb(res): return result

            currentsize -= fstat.st_size
            res = self.Verify_size(target, currentsize)
            if not result.absorb(res): return result

        return result.success()

    def Recursively_remove_files(self, target):
        ensure_safe_path(target)
        desc = """Recursively delete the contents of '{0}'""".format(target)
        result = self.new_result(desc)

        res = run_gfal_expect_ok(self.ctx.opendir, [target])
        if not result.absorb(res): return result
        dirp = res.status.output

        while True:
            res = run_gfal_expect_ok(dirp.readpp, [])
            if not result.absorb(res): return result
            (dirent, fstat) = res.status.output
            if not dirent: break

            if stat.S_ISDIR(fstat.st_mode):
                res = self.Recursively_remove_files(path_join(target, dirent.d_name))
                if not result.absorb(res): return result
                res = self.Remove_directory(path_join(target, dirent.d_name))
                if not result.absorb(res): return result
            else:
                res = self.Remove_file(path_join(target, dirent.d_name))
                if not result.absorb(res): return result

        return result.success()

    def Remove_directory(self, target):
        ensure_safe_path(target)
        return self.forward(run_gfal_expect_ok(self.ctx.rmdir, [target]))

    def Upload_files_in_parallel(self, sources, directory):
        functions = [self.Upload_testfile] * len(sources)
        arguments = []
        for i in range(0, len(sources)):
            arguments.append([add_file_prefix(sources[i]), path_join(directory, "f{0}".format(i))])

        return hammer_tester(functions, arguments)

    def Upload_delete_loop(self, source, destination, ntimes):
        result = self.new_result()
        result.write(f("{source} => {destination}"))
        result.write("Not all performed operations are shown. This test could run "
                     "the loop hundrends of thousands of times, and displaying them all would be a bad idea. "
                     "Only the first failure will be shown. If empty, assume everything worked.")

        for i in range(0, ntimes):
            res = self.Upload_testfile(source, destination)
            if not res.ok():
                result.absorb(res)
                return result

            res = self.Remove_file(destination)
            if not res.ok():
                result.absorb(res)
                return result
        return result.success()

def getargs():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Verifies the correct operation of a DPM instance using gfal2.\n",
                                     epilog="The currently supported sets of tests are:\n"
                                            "\tdavs, root, gsiftp, srm: runs a series of tests using the corresponding protocol\n"
                                            "\tcombined: combines protocols together to verify directory space accounting\n"
                                            "\tdome: runs dome-specific tests.")

    parser.add_argument('--host', type=str, required=True, help="The hostname of the head node")
    parser.add_argument('--path', type=str, default="/dpm/{host_domain}/home/dteam/", help="The base path")
    parser.add_argument('--testdir', type=str, default="dpm-tests", help="The directory in which all tests will be performed. (should not exist beforehand)")
    parser.add_argument('--timeout', type=int, default=30, help="Test timeout")
    parser.add_argument('--tests', type=str, nargs="+", default=["davs", "root", "gsiftp"], help="The sets of tests to run.")
    parser.add_argument('--cert', type=str, help="The path to the certificate to use - key must be supplied seperately.")
    parser.add_argument('--key', type=str, help="The path to the key to use - certificate must be supplied seperately.")

    parser.add_argument('--cleanup', dest='cleanup', action='store_true', help="Do perform cleanup of testfiles (on by default)")
    parser.add_argument('--no-cleanup', dest='cleanup', action='store_false', help="Don't perform any cleanup of testfiles")
    parser.set_defaults(cleanup=True)

    parser.add_argument('--colors', dest='colors', action='store_true', help="Show colored terminal output (default)")
    parser.add_argument('--no-colors', dest='colors', action='store_false', help="Don't show colored terminal output")
    parser.set_defaults(colors=True)

    parser.add_argument('--xml', dest='xml', action='store_true', help="Print XML output in a JUnit compatible format")
    parser.set_defaults(xml=False)

    parser.add_argument('--verbose', dest='verbose', action='store_true', help="Verbose output")

    parser.add_argument('--davs-port', type=int, default=443, help="Supply this option if you have configured https on a non-default port")
    parser.add_argument('--root-port', type=int, default=1094, help="Supply this option if you have configured xrootd on a non-default port")
    parser.add_argument('--gsiftp-port', type=int, default=2811, help="Supply this option if you have configured gsiftp on a non-default port")
    parser.add_argument('--srm-port', type=int, default=8446, help="Supply this option if you have configured srm on a non-default port")

    parser.add_argument('--dome-nchecksums', type=int, default=10, help="Number of parallel checksum calculations to launch in the dome test. Use 0 to disable.")
    parser.add_argument('--dome-poolname', type=str, help="Supply the name of a pool for quotatoken testing - it's important the pool is configured with a defsize of 1.")

    parser.add_argument('--hammer-parallel-uploads', type=int, default=10, help="The hammer test will upload that many files "
                        "in parallel. Warning: setting this option too high can incur heavy load on the entire storage system. "
                        "It is not recommended to use a high value when testing production instances. You might also want to increase "
                        "the timeout value. Use 0 to disable entirely.")

    parser.add_argument('--upload-delete-loop', type=int, default=5, help="The upload-delete-loop test will upload and delete the same file that many times, under the same filename. "
                                                                    " Run with a high number to detect memory leaks.")

    parser.add_argument('--enable-dir-accounting', dest='dir_accounting', action='store_true',
                        help="Enable directory accounting tests. (only works on very recent versions of DPM)")
    parser.set_defaults(dir_accounting=False)

    parser.add_argument('--ignore-errors', dest='ignore_errors', action='store_true')
    parser.set_defaults(ignore_errors=False)

    args = parser.parse_args()
    if not args.path.endswith("/"):
        args.path += "/"

    if not "dpm-test" in args.testdir and not "dpmtest" in args.testdir:
        parser.error("Invalid --testdir: Since this script will essentially run 'rm -rf' "
                     "on testdir at the end, in the interest of preventing accidents "
                     "it is required that testdir contains the string 'dpm-test' or 'dpmtest'. "
                     "Try specifying 'my_dpmtests' or 'dpm-test-2', for example.")

        # if you're editing this source file to remove the above restriction, please:
        # 1. Contact us at dpm-users-forum at cern.ch to let us know what is your use case that
        #    prevents you from having 'dpmtest' in your testdir
        # 2. Make sure to also change function ensure_safe_path ..

    if not re.match(r'^[a-zA-Z0-9_-]+$', args.testdir):
        parser.error("Invalid --testdir: Special characters not permitted here.")

    testchoices = ["davs", "root", "gsiftp", "srm", "dome", "combined"]
    for t in args.tests:
        if t not in testchoices:
            parser.error("unrecognized test name: {0}. Available choices: {1}".format(t, testchoices))

    return args

def delete_file_if_exists(filename):
    if os.path.isfile(filename):
        os.unlink(filename)

def calculate_checksum(checksumtype, filename):
    if checksumtype == "md5":
        return hashlib.md5(open(filename, 'rb').read()).hexdigest()

# Run a series of tests, optionally with initialization and cleanup.
# You can also nest orchestrators together, but be careful.
class Orchestrator:
    def __init__(self, prefix=""):
        self.prefix = prefix
        self.initialization = []
        self.tests = []
        self.cleanup = []

        self.outcome = EX_UNKNOWN

    # if any of the initialization tests fail, no cleanup is necessary
    def add_initialization(self, function, args=[], name=""):
        self.initialization.append((function, args, name))

    # add a regular test
    def add(self, function, args=[], name=""):
        self.tests.append((function, args, name))

    # add cleanup function
    def add_cleanup(self, function, args=[], name=""):
        self.cleanup.append((function, args, name))

    # run list of tests
    def run_list(self, test_list, always_run=False):
        alive = True

        for test in test_list:
            if (alive and not CANCEL) or always_run:
                if test[2]: TestResult.show_pending(self.prefix, test[2])
                result = test[0](*test[1])
                alive = result.ok() or TEST_IGNORE_ERRORS

                # If a name was given, override the result object returned by
                # the function
                if test[2]:
                    result.reset_name(self.prefix, test[2])
                result.show()
        return alive

    # run everything
    def run(self):
        # if initialization fails, no need to cleanup
        if CANCEL or not self.run_list(self.initialization):
            return self

        # run main tests
        if not self.run_list(self.tests):
            self.outcome = EX_CRITICAL

        # run cleanup
        if TEST_CLEANUP and not self.run_list(self.cleanup, always_run=True):
            self.outcome = EX_CRITICAL

        if self.outcome != EX_CRITICAL:
            self.outcome = EX_OK

        return self

    # show a breakdown of results
    def show(self):
        pass

    def ok(self):
        return self.outcome == EX_OK

# add file:// prefix
def add_file_prefix(s):
    return "file://{0}".format(s)

# returns an orchestrator which runs a series of tests against a particular file
def play_with_file(scope, tester, source, destination):
    local_testfile = "/tmp/dpm-tests-tempfile"
    st = os.stat(source)

    orch = Orchestrator(scope)

    descr = "Upload to testdir: " + extract_file(destination)
    orch.add_initialization(tester.Upload_testfile, [add_file_prefix(source), destination], descr)

    descr = "Download from testdir: " + extract_file(destination)
    orch.add(tester.Download_testfile, [destination, add_file_prefix(local_testfile)], descr)

    descr = "Verify downloaded contents are identical"
    orch.add(tester.Verify_downloaded_contents_are_the_same, [source, local_testfile], descr)

    descr = "Verify size is " + str(st.st_size)
    orch.add(tester.Verify_size, [destination, st.st_size], descr)

    # root in gfal does not support checksums
    if not destination.startswith("root://"):
        sourcechksum = calculate_checksum("md5", source)
        descr = "Verify md5 checksum: " + sourcechksum
        orch.add(tester.Verify_checksum, [destination, "md5", sourcechksum], descr)

    descr = "Remove: " + extract_file(destination)
    orch.add_cleanup(tester.Remove_file, [destination], descr)
    return orch

# returns an orchestrator which performs space accounting
# with TWO different protocols.
# Uploading will be performed using one protocol, and deleting with the other.
# During both uploading and deleting verification checks will confirm the
# directory space is what we expect it to be.
# testdir: url with which to upload
# testdir2: url with which to delete
def test_space_accounting(scope, tester, testdir, testdir2=None):
    # using common protocol?
    if not testdir2:
        testdir2 = testdir

    myfile = "/etc/services"
    st = os.stat(myfile)
    nfiles = 10
    totalsize = nfiles * st.st_size

    orch = Orchestrator(scope)

    descr = "Create directory: " + extract_path(testdir)
    orch.add_initialization(tester.Create_directory, [testdir], descr)

    descr = "Upload {0} files, verify accounting: {1}".format(nfiles, extract_path(testdir))
    orch.add(tester.Verify_directory_space_accounting, [add_file_prefix(myfile), testdir, nfiles], descr)

    descr = "Verify listing, {0} files: {1}".format(nfiles, extract_path(testdir))
    orch.add(tester.Verify_listing, [testdir2, [str(x) for x in range(0, nfiles)], totalsize], descr)

    descr = "Remove files, verify accounting: " + extract_path(testdir)
    orch.add(tester.Remove_files_and_verify_space_accounting, [testdir2, totalsize], descr)

    descr = "Remove directory: " + extract_path(testdir)
    orch.add_cleanup(tester.Remove_directory, [testdir2], descr)

    return orch

def cross_protocol_tests(args):
    tester = ProtocolTester("combined")
    protocols = ["davs", "root", "gsiftp"]

    # use xrootd to create and delete the final directories
    testdir = build_target_url(args, "root")

    orch = Orchestrator("combined")

    descr = "Create directory: " + extract_path(testdir)
    orch.add(tester.Create_directory, [testdir], descr)

    for t1 in protocols:
        for t2 in protocols:
            if t1 not in protocols or t2 not in protocols or t1 == t2:
                continue

            foldername = "{0}-vs-{1}".format(t1, t2)
            target1 = path_join(build_target_url(args, t1), foldername)
            target2 = path_join(build_target_url(args, t2), foldername)

            orch.add(test_space_accounting("combined", tester, target1, target2).run)

    descr = "Remove directory: " + extract_path(testdir)
    orch.add_cleanup(tester.Remove_directory, [testdir], descr)
    orch.run()

def build_base_url(args, scheme):
    # determine port
    if scheme == "davs":
        port = args.davs_port
    elif scheme == "root":
        port = args.root_port
    elif scheme == "gsiftp":
        port = args.gsiftp_port
    elif scheme == "srm":
        port = args.srm_port
    else:
        raise ValueError("invalid argument")

    path = build_base_path(args)

    if scheme == "srm":
        return "{0}://{1}:{2}/srm/managerv2?SFN={3}".format(scheme, args.host, port, path)

    return "{0}://{1}:{2}{3}".format(scheme, args.host, port, path)

def build_base_path(args):
    host_domain = ".".join(args.host.split(".")[-2:])

    # if the user has overriden the default args.path, {host_domain}
    # might not be there - that's perfectly ok
    return args.path.format(host_domain=host_domain)

def build_target_url(args, scheme):
    return path_join(build_base_url(args, scheme), args.testdir)

def extract_path(url):
    return urllib.unquote(urlparse(url).path)

def extract_file(url):
    return urllib.unquote(url.split("/")[-1])

def single_protocol_tests(args, scope):
    tester = ProtocolTester(scope)
    orch = Orchestrator(scope)

    base = build_base_url(args, scope)
    target = build_target_url(args, scope)

    descr = "Verify base exists: " + extract_path(base)
    orch.add_initialization(tester.Verify_base_directory_exists, [base], descr)

    descr = "Verify testdir does not exist: " + extract_path(target)
    orch.add(tester.Verify_directory_does_not_exist, [target], descr)

    descr = "Create testdir: " + extract_path(target)
    orch.add(tester.Create_directory, [target], descr)

    # adds another orchestrator
    orch.add(play_with_file(scope, tester, "/etc/services", "{0}/services".format(target)).run)

    if scope != "srm":
        evil_filename = """evil filename-!@#%^_-+=:][}{><'" #$&*)("""
        orch.add(play_with_file(scope, tester, "/etc/services", path_join(target, evil_filename)).run)

        if args.dir_accounting:
            orch.add(test_space_accounting(scope, tester, path_join(target, "spc-accounting")).run)

    nfiles = args.hammer_parallel_uploads
    if nfiles > 0:
        descr = "Hammer test - upload {0} files in parallel: {1}".format(nfiles, extract_path(target))
        orch.add(tester.Upload_files_in_parallel, [ ["/etc/services"]*nfiles, target], descr)

    ntimes = args.upload_delete_loop
    if ntimes > 0:
        descr = f("Upload and delete the same file {ntimes} times")
        orch.add(tester.Upload_delete_loop, ["file:///etc/services", path_join(target, "upload-delete-loop"), ntimes], descr)

    descr = "Recursively remove contents: " + extract_path(target)
    orch.add_cleanup(tester.Recursively_remove_files, [target], descr)

    descr = "Remove directory: " + extract_path(target)
    orch.add_cleanup(tester.Remove_directory, [target], descr)

    orch.run()

def unset_environ(s):
    if s in os.environ:
        del os.environ[s]

# cert, key => filesystem paths to the certificate and key
# if key is empty, that means cert is a proxy and contains both
def set_credentials(cert, key):
    if not cert:
        return

    if key:
        unset_environ("X509_USER_PROXY")
        os.environ["X509_USER_CERT"] = cert
        os.environ["X509_USER_KEY"] = key
        return

    # only cert, set a proxy
    unset_environ("X509_USER_CERT")
    unset_environ("X509_USER_KEY")
    os.environ["X509_USER_PROXY"] = args.cert

def xml_start():
    print("""<?xml version="1.0" encoding="UTF-8"?><testsuite>""")

def xml_end():
    print("""</testsuite>""")

def set_global_flags(args):
    global TEST_TIMEOUT
    TEST_TIMEOUT = args.timeout

    global TEST_CLEANUP
    TEST_CLEANUP = args.cleanup

    global TEST_VERBOSE
    TEST_VERBOSE = args.verbose

    global USE_COLORS
    USE_COLORS = args.colors

    global USE_XML
    USE_XML = args.xml
    if USE_XML: USE_COLORS = False

    global TEST_IGNORE_ERRORS
    TEST_IGNORE_ERRORS = args.ignore_errors

def get_varying_filesizes(n, seed=1):
    ret = [1, 2, 4, 7, 8, 16, 32, 64, 100, 200, 1000, 5000, 15000, 75000, 90000, 1, 50000, 1024*1024, 2*1024*1024, 10*1024*1024-1]

    random.seed(seed)
    for i in range(0, n-len(ret)):
        ret.append(random.randint(1, 10000))

    return ret[0:n]

def test_dome(args):
    domehead = f("https://{args.host}/domehead/")

    orch = Orchestrator("dome")
    dometester = DomeTester(domehead)
    tester = ProtocolTester("dome")

    target = build_target_url(args, "davs")
    tokendir = path_join(target, "tokendir")

    target_path = extract_path(target)

    orch.add_initialization(dometester.info, [])
    orch.add(tester.Create_directory, [target], f("Create directory: {target_path}"))

    # checksum tests
    nfiles = args.dome_nchecksums
    if nfiles > 0:
        sizes = get_varying_filesizes(nfiles)
        (locations, checksums) = create_random_files("/tmp/dpm-tests-checksums", sizes)
        lfns = []

        for i in range(0, len(locations)):
            lfns.append(path_join(target, f("f{i}")))

        interval = 10
        checks = 200

        orch.add(tester.Upload_files_in_parallel, [locations, target],
                "Upload {0} files of varying sizes in parallel".format(len(sizes)))

        orch.add(dometester.parallel_checksums, [lfns, checksums, checks, interval],
                f("Calculate checksums in parallel, {checks} checks spaced {interval} seconds apart"))

    # quotatoken tests
    if args.dome_poolname:
        sizes = [1, 1024*1024, 1024*1024-2, 1024*1024-3]
        (locations, checksums) = create_random_files("/tmp/dpm-tests-qt", sizes)

        tokendir_path = extract_path(tokendir)
        orch.add(tester.Create_directory, [tokendir], f("Create directory: {tokendir_path}"))

        orch.add(dometester.setquotatoken, ["DPM_TESTER_QUOTATOKEN", tokendir_path, args.dome_poolname, 1024*1024])
        orch.add(tester.Upload_testfile, [f("file://{locations[1]}"), path_join(tokendir, "1mb")],
                 "Upload file with size equal to quota")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b"), None, True],
                 "Upload 1-byte file, expect it to fail")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1mb-2"), None, True],
                 "Upload file with size equal to quota again, expect it to fail")

        orch.add(tester.Recursively_remove_files, [tokendir],
                 "Remove any previous files")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b")],
                 "Upload 1-byte file")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b-2")],
                 "Upload 1-byte file")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b-3")],
                 "Upload 1-byte file")

        orch.add(tester.Recursively_remove_files, [tokendir],
                 "Remove any previous files")

        orch.add(tester.Upload_testfile, [f("file://{locations[2]}"), path_join(tokendir, "1mb-m2")],
                 "Upload file with size equal to quota minus 2 bytes")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b")],
                 "Upload 1-byte file")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b-2"), None, True],
                 "Upload 1-byte file, expect it to fail")

        orch.add(tester.Recursively_remove_files, [tokendir],
                 "Remove any previous files")

        orch.add(tester.Upload_testfile, [f("file://{locations[3]}"), path_join(tokendir, "1mb-m3")],
                 "Upload file with size equal to quota minus 3 bytes")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b")],
                 "Upload 1-byte file")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b-2")],
                 "Upload 1-byte file")

        orch.add(tester.Upload_testfile, [f("file://{locations[0]}"), path_join(tokendir, "1b-3"), None, True],
                 "Upload 1-byte file, expect it to fail")

        orch.add_cleanup(dometester.delquotatoken, [tokendir_path, args.dome_poolname])

    orch.add_cleanup(tester.Recursively_remove_files, [target],
                     f("Recursively remove contents: {target_path}"))

    orch.add_cleanup(tester.Remove_directory, [target],
                     f("Remove directory: {target_path}"))

    orch.run()

def main():
    signal.signal(signal.SIGINT, ctrlc)
    args = getargs()

    set_global_flags(args)
    set_credentials(args.cert, args.key)

    if USE_XML: xml_start()

    for testset in args.tests:
        if CANCEL: break

        if testset == "dome":
            test_dome(args)
        elif testset == "combined":
            cross_protocol_tests(args)
        else:
            single_protocol_tests(args, testset)

    if USE_XML: xml_end()

if __name__ == '__main__':
    main()
