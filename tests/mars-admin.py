#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shlex
import subprocess as sub
import time

import sys

try:
    # python 3
    from urllib.parse import quote_plus
except ImportError:
    # python 2
    from urllib import quote_plus
try:
    # python 3
    import configparser
except ImportError:
    # python 2
    import ConfigParser as configparser


def is_windows():
    return sys.platform.startswith('win')


def identity(x):
    return x


def cygpath(x):
    command = ["cygpath", "-wp", x]
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    lines = output.split(os.linesep)
    return lines[0]


def init_mars_admin_env():
    global CLUSTER_CONF_DIR
    ini_file = os.path.join(CLUSTER_CONF_DIR, 'mars_admin_env.ini')
    if not os.path.isfile(ini_file):
        return
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(ini_file)
    options = config.options('environment')
    for option in options:
        value = config.get('environment', option)
        os.environ[option] = value


normclasspath = cygpath if sys.platform == 'cygwin' else identity
MARS_ADMIN_DIR = os.sep.join(os.path.realpath(__file__).split(os.sep)[:-2])
USER_CONF_DIR = os.path.expanduser("~" + os.sep + ".mars")
MARS_ADMIN_CONF_DIR = os.getenv('MARS_ADMIN_CONF_DIR', None)

if MARS_ADMIN_CONF_DIR == None:
    CLUSTER_CONF_DIR = os.path.join(MARS_ADMIN_DIR, "conf")
else:
    CLUSTER_CONF_DIR = MARS_ADMIN_CONF_DIR

if (not os.path.isfile(os.path.join(USER_CONF_DIR, "mars-admin.yaml"))):
    USER_CONF_DIR = CLUSTER_CONF_DIR

MARS_ADMIN_LIB_DIR = os.path.join(MARS_ADMIN_DIR, "lib")
MARS_ADMIN_BIN_DIR = os.path.join(MARS_ADMIN_DIR, "bin")

init_mars_admin_env()

CONFIG_OPTS = []
CONFFILE = ""
JAR_JVM_OPTS = shlex.split(os.getenv('MARS_ADMIN_JAR_JVM_OPTS', ''))
JAVA_HOME = os.getenv('JAVA_HOME', None)
JAVA_CMD = 'java' if not JAVA_HOME else os.path.join(JAVA_HOME, 'bin', 'java')
if JAVA_HOME and not os.path.exists(JAVA_CMD):
    print("ERROR:  JAVA_HOME is invalid.  Could not find bin/java at %s." % JAVA_HOME)
    sys.exit(1)

def get_config_opts():
    global CONFIG_OPTS
    return "-Dmars.admin.options=" + ','.join(map(quote_plus, CONFIG_OPTS))


if not os.path.exists(MARS_ADMIN_LIB_DIR):
    print("******************************************")
    print(
    "The mars client can only be run from within a release. You appear to be trying to run the client from a checkout of Mars' source code.")
    print("\nYou can download a Mars release at http://mars.jd.com/downloads.html")
    print("******************************************")
    sys.exit(1)


def get_jars_full(adir):
    files = []
    if os.path.isdir(adir):
        files = os.listdir(adir)
    elif os.path.exists(adir):
        files = [adir]

    ret = []
    for f in files:
        if f.endswith(".jar"):
            ret.append(os.path.join(adir, f))
    return ret


def get_classpath(extrajars):
    ret = get_jars_full(MARS_ADMIN_DIR)
    ret.extend(get_jars_full(MARS_ADMIN_DIR + "/lib"))
    ret.extend(extrajars)
    return normclasspath(os.pathsep.join(ret))


def confvalue(name, extrapaths):
    global CONFFILE
    command = [
        JAVA_CMD, "-client", get_config_opts(), "-Dmars.admin.conf.file=" + CONFFILE,
        "-cp", get_classpath(extrapaths), "com.jd.mars.admin.utils.ConfigValue", name
    ]
    p = sub.Popen(command, stdout=sub.PIPE)
    output, errors = p.communicate()
    # python 3
    if not isinstance(output, str):
        output = output.decode('utf-8')
    lines = output.split(os.linesep)
    for line in lines:
        tokens = line.split(" ")
        if tokens[0] == "VALUE:":
            return " ".join(tokens[1:])
    return ""


def print_localconfvalue(name):
    """Syntax: [mars localconfvalue conf-name]

    Prints out the value for conf-name in the local Mars configs.
    The local Mars configs are the ones in ~/.mars/mars.yaml merged
    in with the configs in mars-defaults.yaml.
    """
    print(name + ": " + confvalue(name, [USER_CONF_DIR]))


def print_remoteconfvalue(name):
    """Syntax: [mars remoteconfvalue conf-name]

    Prints out the value for conf-name in the cluster's Mars configs.
    The cluster's Mars configs are the ones in $MARS-PATH/conf/mars.yaml
    merged in with the configs in mars-defaults.yaml.

    This command must be run on a cluster machine.
    """
    print(name + ": " + confvalue(name, [CLUSTER_CONF_DIR]))


def parse_args(string):
    """Takes a string of whitespace-separated tokens and parses it into a list.
    Whitespace inside tokens may be quoted with single quotes, double quotes or
    backslash (similar to command-line arguments in bash).

    >>> parse_args(r'''"a a" 'b b' c\ c "d'd" 'e"e' 'f\'f' "g\"g" "i""i" 'j''j' k" "k l' l' mm n\\n''')
    ['a a', 'b b', 'c c', "d'd", 'e"e', "f'f", 'g"g', 'ii', 'jj', 'k k', 'l l', 'mm', r'n\n']
    """
    re_split = re.compile(r'''((?:
        [^\s"'\\] |
        "(?: [^"\\] | \\.)*" |
        '(?: [^'\\] | \\.)*' |
        \\.
    )+)''', re.VERBOSE)
    args = re_split.split(string)[1::2]
    args = [re.compile(r'"((?:[^"\\]|\\.)*)"').sub('\\1', x) for x in args]
    args = [re.compile(r"'((?:[^'\\]|\\.)*)'").sub('\\1', x) for x in args]
    return [re.compile(r'\\(.)').sub('\\1', x) for x in args]


def exec_mars_admin_class(klass, jvmtype="-server", jvmopts=[], extrajars=[], args=[], fork=False):
    global CONFFILE
    mars_admin_log_dir = confvalue("mars.admin.log.dir", [CLUSTER_CONF_DIR])
    if (mars_admin_log_dir == None or mars_admin_log_dir == "null"):
        mars_admin_log_dir = os.path.join(MARS_ADMIN_DIR, "logs")
    mars_admin_native_dir = confvalue("java.library.path", [CLUSTER_CONF_DIR])
    if (mars_admin_native_dir == None or mars_admin_native_dir == "null"):
        mars_admin_native_dir = os.path.join(MARS_ADMIN_DIR, "native")
    all_args = [
                   JAVA_CMD, jvmtype,
                   get_config_opts(),
                   "-Dmars.admin.home=" + MARS_ADMIN_DIR,
                   "-Dmars.admin.log.dir=" + mars_admin_log_dir,
                   "-Djava.library.path=" + mars_admin_native_dir,
                   "-Dmars.admin.conf.file=" + CONFFILE,
                   "-cp", get_classpath(extrajars),
               ] + jvmopts + [klass] + list(args)
    print("Running: " + " ".join(all_args))
    sys.stdout.flush()
    exit_code = 0
    proc = None
    if fork:
        #exit_code = os.spawnvp(os.P_WAIT, JAVA_CMD, all_args)
        # modify here for no
        proc = sub.Popen(all_args)
        time.sleep(2)
        if proc.poll() != None: # terminated
            exit_code = proc.returncode
    elif is_windows():
        # handling whitespaces in JAVA_CMD
        try:
            # proc = sub.check_output(all_args, stderr=sub.STDOUT)
            # modify
            proc = sub.Popen(all_args, stderr=sub.STDOUT)
            # print(proc)
        except sub.CalledProcessor as e:
            sys.exit(e.returncode)
    else:
        # os.execvp(JAVA_CMD, all_args)
        # modify
        proc = sub.Popen(all_args)
        time.sleep(2)
        if proc.poll() != None:  # terminated
            exit_code = proc.returncode
    pid = proc.pid
    # write the pid to the file
    f = open('logs/mars.pid', 'w')
    f.write(pid)
    f.close()
    return exit_code

def admin(klass="com.jd.mars.admin.MarsAdmin"):
    """Syntax: [mars nimbus]

    Launches the nimbus daemon. This command should be run under
    supervision with a tool like daemontools or monit.

    See Setting up a Storm cluster for more information.
    (http://mars.apache.org/documentation/Setting-up-a-Storm-cluster)
    """
    cppaths = [CLUSTER_CONF_DIR]
    jvmopts = parse_args(confvalue("mars.admin.childopts", cppaths)) + [
        "-Dlogfile.name=mars-admin.log",
        "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
        "-Dlog4j.configurationFile=" + os.path.join(MARS_ADMIN_CONF_DIR, "mars-admin-log4j2.xml"),
    ]
    exec_mars_admin_class(
        klass,
        jvmtype="-server",
        extrajars=cppaths,
        jvmopts=jvmopts)

def version():
    """Syntax: [mars version]

    Prints the version number of this Storm release.
    """
    cppaths = [CLUSTER_CONF_DIR]
    exec_mars_admin_class(
        "com.jd.mars.admin.utils.VersionInfo",
        jvmtype="-client",
        extrajars=cppaths)


def print_classpath():
    """Syntax: [mars classpath]

    Prints the classpath used by the mars client when running commands.
    """
    print(get_classpath([]))


def print_commands():
    """Print all client commands and link to documentation"""
    print("Commands:\n\t" + "\n\t".join(sorted(COMMANDS.keys())))
    print("\nHelp: \n\thelp \n\thelp <command>")
    print(
    "\nDocumentation for the mars client can be found at http://mars.apache.org/documentation/Command-line-client.html\n")
    print(
    "Configs can be overridden using one or more -c flags, e.g. \"mars list -c nimbus.host=nimbus.mycompany.com\"\n")


def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if command in COMMANDS:
            print(COMMANDS[command].__doc__ or
                  "No documentation provided for <%s>" % command)
        else:
            print("<%s> is not a valid command" % command)
    else:
        print_commands()


def unknown_command(*args):
    print("Unknown command: [mars %s]" % ' '.join(sys.argv[1:]))
    print_usage()
    sys.exit(254)


COMMANDS = {"admin": admin, "localconfvalue": print_localconfvalue,
            "remoteconfvalue": print_remoteconfvalue, "classpath": print_classpath,
            "help": print_usage, "version": version}


def parse_config(config_list):
    global CONFIG_OPTS
    if len(config_list) > 0:
        for config in config_list:
            CONFIG_OPTS.append(config)


def parse_config_opts(args):
    curr = args[:]
    curr.reverse()
    config_list = []
    args_list = []

    while len(curr) > 0:
        token = curr.pop()
        if token == "-c":
            config_list.append(curr.pop())
        elif token == "--config":
            global CONFFILE
            CONFFILE = curr.pop()
        else:
            args_list.append(token)

    return config_list, args_list


def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    global CONFIG_OPTS
    config_list, args = parse_config_opts(sys.argv[1:])
    parse_config(config_list)
    COMMAND = args[0]
    ARGS = args[1:]
    (COMMANDS.get(COMMAND, unknown_command))(*ARGS)


if __name__ == "__main__":
    main()
