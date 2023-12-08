#!/usr/bin/env python

# (c) 2013, A. Riva, University of Florida
# $Id: submit.py,v 1.6 2013/12/07 03:54:43 alb Exp $

import os, os.path
import sys
import glob
import fcntl
import getpass
import subprocess as sp
from datetime import datetime
from smtplib import SMTP

PYVER = sys.version_info.major

# Error codes:
# 1 - invalid mode specified
# 2 - sbatch error (wrong submission params)
# 3 - file specified with -T does not exist
# 4 - bad syntax (missing script name, etc)
# 5 - script not found
# 6 - any other error

# Utils

def countLines(filename):
    n = 0
    with open(filename, "r") as f:
        for _ in f:
            n += 1
    return n

def parseDirList(dl):
    dirs = []
    for d in dl.split(":"):
        if os.path.isdir(d):
            dirs.append(d)
    return dirs

# Main class

class Submit():
    mode     = "slurm"         # Or "pbs"
    dry      = False
    debug    = 0
    decorate = True
    doneFile = None
    array    = None
    comment  = None
    queue    = None
    coptions = []            # From cmdline -o option
    foptions = None            # From confFile
    fileArray = None           # from -T
    arrayArgs = False          # Set by -A
    sendEmail = False          # Set by -m
    emailAlways = False        # If True, send email also for successful jobs (set by -M)
    logdir = False             # Directory where .IN.o and .IN.e files are written
    
    confFile  = ".sbatchrc"
    logFile   = os.path.dirname(__file__) + "/../lib/submit.log"
    dbFile    = os.path.dirname(__file__) + "/../lib/sdb/submit.db"
    useDB     = True
    trueArgs  = []
    afterArgs = []
    scriptLibrary = []
    
    def __init__(self, mode):
        self.mode = mode
        self.confFile = DEFAULTS[mode]['conf']
        submitlib = os.getenv("SUBMIT_LIB")
        if submitlib:
            self.scriptLibrary = parseDirList(submitlib)
        else:
            self.scriptLibrary = [os.path.dirname(__file__) + "/../lib/scripts/"]

    def dump(self):
        for a in ['mode', 'dry', 'decorate', 'doneFile', 'array', 'comment', 'queue', 'coptions', 'foptions', 'confFile', 'scriptLibrary', 'logFile', 'afterArgs', 'trueArgs']:
            sys.stderr.write("{} = {}\n".format(a, getattr(self, a)))

    def writeLogEntry(self, script, jobid):
        """Write a log entry to the logFile to record that `script' was called. Uses locking."""
        now = datetime.now()
        scriptname = os.path.split(script)[1]
        cwd = os.getcwd()
        try:
            with open(self.logFile, "a") as f:
                fcntl.flock(f,fcntl.LOCK_EX)
                try:
                    f.write(now.isoformat('\t') + '\t' + str(jobid) + "\t" + getpass.getuser() + '\t' + scriptname + "\t" + cwd + '\n')
                finally:
                    fcntl.flock(f,fcntl.LOCK_UN)
        except:
            sys.stderr.write("Warning: log file `{}' does not exist or is not writable.\n".format(self.logFile))

    def usage(self):
        if self.mode == "slurm":
            fargs = {'progname': sys.argv[0],
                     'sub': "sbatch",
                     'args': "$1, $2, $3, etc",
                     'conf': self.confFile}
        elif self.mode == "pbs":
            fargs = {'progname': sys.argv[0],
                     'sub': "squeue",
                     'args': "$arg1, $arg2, $arg3, etc",
                     'conf': self.confFile}

        sys.stdout.write("""{progname} - submit jobs to a cluster scheduler.

Usage: submit [submit-options] scriptName [arguments...]
       submit -w jobids...
       submit -ls
       submit -v[v[v]] scriptName

Submits script `scriptName' using {sub}, passing the values of `arguments'
to the script as {args}. The script should be in the current directory 
or in the scripts library (see the -ls option).

Submit options (should be BEFORE script name):  

 -conf file   | Read additional {sub} options from `file' (default: "{conf}").
              | The path is relative to your home directory: for example, 
              | "-conf confs/largejob.txt" will read options from the "largejob.txt"
              | file in the confs/ subdirectory of your home. Set this argument to 
              | a non-existent file to disable option loading.

 -after jobid | The script will run after the job indicated by "jobid" has 
              | terminated successfully. Multiple "-after" arguments may
              | be specified. If `jobid' is 0, it will be ignored.

 -done name   | The script will create a file called "name" when it terminates.
              | This can be used to detect that execution of the script has 
              | finished. If the file name contains the `@' character, it will 
              | be replaced with the job ID.

 -p comment   | Associate `comment' with with the submitted job. This is useful 
              | to distinguish instances of the same script submitted for different 
              | projects. Cannot be combined with "-n".

 -q queue     | Passed to {sub} as the -A argument, to specify a destination queue.

 -t arrspec   | Passed to {sub} as the -a argument, to specify a job array. For
              | example: "-t 0-15%4" will run an array of 16 jobs, numbered from
              | 0 to 15, with the constraint that at most 4 of them will run at
              | the same time.

 -T arrfile   | Creates a job array with one job for each line in "arrfile". In the
              | job script, the line can be accessed using the JOB_FILEARRAY_LINE
              | variable. The % symbol can be used to limit the number of concurrent
              | jobs, as in -a.

 -A arrfile   | Like -T, but arrfile can be a tab-delimited file, and its contents
              | are stored in the JOB_FILEARRAY_ARGS array. For example, the contents
              | of the second column are accessed as ${{JOB_FILEARRAY_ARGS[1]}}.

 -W           | Cause submit to wait until the job is done before returning.

 -o options   | Pass `options' to the {sub} command-line. The options should be 
              | separated by commas, with no spaces between them. For example: 
              | "-o --mem=10G,--time=20:00:00".

 -n           | Submit will NOT modify the script (by default, submit will add 
              | messages printing start and stop times for the script). This 
              | option is incompatible with "-done" and "-n".

 -x           | Only print submission command, do not execute it.

 -mode sched  | Use syntax for the `sched' scheduler. Possible values are "slurm"
              | (the default) and "pbs". The mode can also be set by assigning a
              | value to the SUBMIT_MODE environment variable. The command-line
              | option takes precedence over the environment variable.

 -lib libdirs | Use `libdirs' as the search path for scripts. Default: "../lib/scripts"
              | (relative to location of this command). This can also be set
              | by assigning a value to the SUBMIT_LIB environment variable. The
              | command-line option takes precedence over the environment variable.

 -log logfile | Record job submissions to `logfile'. Degault: "../lib/submit.log"
              | (relative to location of this command).

 -l directory | Directory where the stdout and stderr files are written. Defaults
              | to a subdirectory called 'log' of the current directory, if it
              | exists, or the current directory.

Other options:

 -w jobids...  | Extract log file entry for each of the specified job ids.

 -ls [patt...] | List all the available *.qsub scripts in the default library directory. 
               | If one or more `patt' arguments are specified, only lists scripts
               | whose name contains one of them; otherwise, all scripts are listed.
               | For example, `submit -ls bowtie' will list all scripts with `bowtie' in
               | their name.  
               | The location of the library is printed before the list of scripts. 

 -v[v[v]] name | Print a one-line description of what script `name' does; "-vv" also 
               | prints a description of the command-line arguments accepted by the 
               | script, and "-vvv" prints out the entire script.

 -d[d]         | Debug mode: print command line being executed. "-dd" also displays
               | parameter values and does not delete decorated script.

Return value:

This command prints the id of the submitted job, which is suitable as the -after 
argument for a subsequent job. For example:

  STEP1=`submit.py step1.qsub`
  STEP2=`submit.py -after $STEP1 step2.qsub`

Configuration:

  SUBMIT_MODE - environment variable containing submit mode (slurm or pbs)
  SUBMIT_LIB - search path for qsub scripts (colon-separated list of directories)
  {conf} - file in home directory containing default submit options

(c) 2014-2023, Alberto Riva, DiBiG, ICBR Bioinformatics Core, University of Florida.
""".format(**fargs))

    def parseArgs(self, args):
        mode = "submit"
        after = False
        prev = ""
        valuedArgs = ['-v', '-vv', '-vvv', '-conf', '-after', '-done', '-n', '-p', '-q', '-t', '-T', '-A', '-o', '-lib', '-log', '-mode', '-m', '-M', '-l']

        if args == []:
            self.usage()
            return False

        for a in args:
            if after:
                self.trueArgs.append(a)
            else:
                if a == "-ls":
                    mode = "list"
                elif a == "-w":
                    mode = "lookup"
                elif a == "-em":
                    mode = "email"
                elif a == "-n":
                    self.decorate = False
                elif a == "-x":
                    self.dry = True
                elif a == "-d":
                    self.debug = 1
                elif a == "-dd":
                    self.debug = 2
                elif a in ["-h", "--help"]:
                    self.usage()
                    return False
                elif prev == "-mode":
                    prev = ""   # -mode has already been processed
                elif prev in ['-v', '-vv', '-vvv']:
                    self.viewScript(a, prev)
                    return False
                elif prev == "-conf":
                    self.confFile = a
                    prev = ""
                elif prev == "-after":
                    if a != "0":
                        self.afterArgs.append(a)
                    prev = ""
                elif prev == "-done":
                    self.doneFile = a
                    prev = ""
                elif prev == "-p":
                    self.comment = a
                    prev = ""
                elif prev == "-q":
                    self.queue = a
                    prev = ""
                elif prev == "-t":
                    self.array = a
                    prev = ""
                elif prev == "-T":
                    self.fileArray = a
                    prev = ""
                elif prev == "-A":
                    self.fileArray = a
                    self.arrayArgs = True
                    prev = ""
                elif prev == "-o":
                    self.coptions.append(a.replace(",", " "))
                    prev = ""
                elif prev == "-lib":
                    self.scriptLibrary = parseDirList(a)
                    prev = ""
                elif prev == "-log":
                    self.logFile = a
                    prev = ""
                elif prev == "-m":
                    self.sendEmail = a
                    prev = ""
                elif prev == "-M":
                    self.sendEmail = a
                    self.emailAlways = True
                    prev = ""
                elif prev == '-l':
                    self.logdir = a
                    prev = ""
                elif a in valuedArgs:
                    prev = a 
                elif a == "-W":
                    self.coptions.append("-W")
                else:
                    self.trueArgs.append(a)
                    after = True

        if mode == "list":
            self.listScripts(self.trueArgs)
            return False

        if mode == "lookup":
            self.lookupJobs(self.trueArgs)
            return False

        if mode == "email":
            self.sendNotification(self.trueArgs)
            return False

        if self.fileArray:
            parts = self.fileArray.split("%")
            faname = parts[0]
            if not os.path.isfile(faname):
                sys.stderr.write("Error: file `{}' does not exist.\n".format(faname))
                sys.exit(3)
            njobs = countLines(faname)
            self.array = "1-" + str(njobs)
            if len(parts) > 1:
                self.array += "%" + parts[1]
            self.fileArray = faname
            #sys.stderr.write(self.fileArray + "\n")
            #sys.stderr.write(self.array + "\n")

        if self.trueArgs:
            return True
        else:
            sys.stderr.write("Error: missing script name. Use -h for help.\n")
            sys.exit(4)

    def readOptions(self):
        optpath = os.path.expanduser("~/" + self.confFile)
        #sys.stderr.write("Reading options from " + optpath + "\n")
        if os.path.isfile(optpath):
            with open(optpath, 'r') as f:
                opts = f.read()
            opts = opts.replace('\n', ' ')
            self.foptions = opts.replace('\r', ' ')
            #sys.stderr.write("Read: " + self.foptions + "\n")
        return self.foptions

    def setVars(values):
        idx = 1
        names = ["args"]
        os.putenv("args", " ".join(values))
        
        for v in values:
            name = "arg" + str(idx)
            os.putenv(name, v)
            names.append(name)
            idx = idx + 1

        return names

    def decorateScript(self, infile, out):
        dirtag = DEFAULTS[self.mode]['directive']
        inHeader = True
        with open(infile, "r") as inf:
            for row in inf:
                if inHeader:
                    srow = row.strip()
                    if len(srow) == 0 or srow.startswith("#!") or srow.startswith(dirtag):
                        pass
                    else:
                        inHeader = False
                        out.write("\necho %Commandline: " + " ".join(self.trueArgs) + "\n")
                        out.write("echo %Started: `date`\n")
                        out.write("_ORIG_PWD=$PWD\n")
                        out.write("_SUBMIT_TS=$(date +%s)\n\n")
                        if self.fileArray:
                            if self.arrayArgs:
                                out.write("""JOB_FILEARRAY_ARGS=($(sed "${{SLURM_ARRAY_TASK_ID}}q;d" {}))\n""".format(self.fileArray))
                            else:
                                out.write("""JOB_FILEARRAY_LINE=$(sed "${{SLURM_ARRAY_TASK_ID}}q;d" {})\n""".format(self.fileArray))
                out.write(row)
            out.write("_RETCODE=$?\n")
            if self.doneFile:
                p = self.doneFile.find("@")
                if p >= 0:
                    self.doneFile = self.doneFile[0:p] + DEFAULTS[self.mode]['jobid'] + self.doneFile[p+1:]
                #out.write("touch " + self.doneFile + "\n")
                out.write("echo $_RETCODE > {}\n".format(self.doneFile))
            out.write("echo %Terminated: `date`\n")
            out.write("_SUBMIT_TS2=$(date +%s)\n")
            out.write("echo %Elapsed: $(($_SUBMIT_TS2 - $_SUBMIT_TS)) seconds\n")
            if self.sendEmail:
                if self.emailAlways:
                    out.write("""{}/submit -em {} $SLURM_JOB_ID $_RETCODE $(($_SUBMIT_TS2 - $_SUBMIT_TS)) $_ORIG_PWD "{}"\n""".format(os.path.dirname(__file__), self.sendEmail, " ".join(self.trueArgs)))
                else:
                    out.write("""if [[ "$_RETCODE" != "0" ]]; then {}/submit -em {} $SLURM_JOB_ID $_RETCODE $(($_SUBMIT_TS2 - $_SUBMIT_TS)) $ORIG_PWD "{}"; fi\n""".format(os.path.dirname(__file__), self.sendEmail, " ".join(self.trueArgs)))
            out.write("exit $_RETCODE\n")

    def resolveScriptName(self, scriptName):
        if os.path.isfile(scriptName):
            return scriptName
        for dl in self.scriptLibrary:
            scriptPath = dl + "/" + scriptName
            if os.path.isfile(scriptPath):
                return scriptPath
        sys.stderr.write("Error: script `{}' not found either in current directory or in script library!\n(Script library: {})\n".format(scriptName, self.scriptLibrary))
        sys.exit(5)

    def makeCmdline(self):
        origName = self.trueArgs[0]
        filename = os.path.split(origName)[1]
        if self.logdir:
            logdir = self.logdir + "/"
        else:
            logdir = "log/" if os.path.isdir("log") else ""
        # print (name, origName, filename)
        cmdline = 'sbatch --parsable -D "`pwd`" -J ' + origName
        if self.array:
            cmdline += " -o {}{}.o%A_%a -e {}{}.e%A_%a -a {}".format(logdir, filename, logdir, filename, self.array)
        else:
            cmdline += " -o {}{}.IN.o%j -e {}{}.IN.e%j".format(logdir, filename, logdir, filename)
        if self.comment:
            cmdline += ' --comment "{}"'.format(self.comment)
        if self.afterArgs:
            aspecs = [ "afterok:" + a for a in self.afterArgs ]
            cmdline += " -d " + ",".join(aspecs)
        if self.queue:
            cmdline += " -A " + self.queue
        if self.foptions:
            cmdline += " " + self.foptions
        if self.coptions:
            cmdline += " " + " ".join(self.coptions)

        for i in range(len(self.trueArgs)):
            if " " in self.trueArgs[i]:
                self.trueArgs[i] = '"' + self.trueArgs[i] + '"'

        return cmdline + " /dev/stdin " + " ".join(self.trueArgs[1:])

    def doCollect(self, jobid, name, arg1):
        cmdline = "scollect.py submit -db {} {} {} {}".format(self.dbFile, jobid, name, arg1)
        try:
            subprocess.check_output(cmdline, shell=True)
        except:
            pass

    def submitScript(self, cmdline, script):
        if PYVER == 3:
            proc = sp.Popen(cmdline, shell="True", stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')
        else:
            proc = sp.Popen(cmdline, shell="True", stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
        if self.decorate:
            self.decorateScript(script, proc.stdin)
        else:
            with open(script, "r") as f:
                proc.stdin.write(f.read())
        proc.stdin.close()
        proc.wait()
        result = proc.stdout.readline().rstrip("\r\n")
        error = proc.stderr.read()
        if error:
            sys.stderr.write(error)
            sys.exit(2)
            return 0
        return int(result)

    def main(self):
        origScript = self.resolveScriptName(self.trueArgs[0])
        if origScript:
            self.readOptions()
            cmdline = self.makeCmdline()

            if self.debug > 0:
                sys.stderr.write("Executing: " + cmdline + "\n")
            if self.debug > 1:
                self.decorateScript(origScript, sys.stderr)
                self.dry = True
            if not self.dry:
                if not os.access(".", os.X_OK | os.W_OK):
                    sys.stderr.write("Warning: current directory is not writeable, log files for this job will not be created.\n")
                jobid = self.submitScript(cmdline, origScript)
                if jobid > 0:
                    sys.stdout.write(str(jobid) + "\n")
                    self.writeLogEntry(origScript, jobid)

    # def main(self):
    #     (origScript, decScript) = self.resolveScriptName(self.trueArgs[0])
    #     if origScript:
    #         if self.decorate:
    #             self.decorateScript(origScript, decScript)
    #             toRun = decScript
    #         else:
    #             toRun = origScript
    #         self.readOptions()
    #         cmdline = self.makeCmdline(toRun)
    #         if self.debug > 0:
    #             sys.stderr.write("Executing: " + cmdline + "\n")
    #         if not self.dry:
    #             #os.system(cmdline)
    #             jobid = subprocess.check_output(cmdline, shell=True).rstrip("\n")
    #             if self.useDB:
    #                 if len(self.trueArgs) > 1:
    #                     arg1 = self.trueArgs[1]
    #                 else:
    #                     arg1 = ""
    #                 self.doCollect(jobid, toRun, arg1)
    #             sys.stdout.write(jobid + "\n")
    #             self.writeLogEntry(toRun, jobid)
    #         if self.decorate and self.debug < 2:
    #             os.remove(decScript)

    ### Additional commands

    def listScripts(self, patterns=[]):
        files = glob.glob("{}/*.qsub".format(self.scriptLibrary))
        files = [ os.path.split(f)[1] for f in files ]
        files.sort()
        sys.stdout.write("Scripts in {}:\n".format(self.scriptLibrary))
        for f in files:
            if self.matches(f, patterns):
                sys.stdout.write("  " + f + "\n")
        sys.stdout.write("\n")

    def matches(self, name, patterns):
        if not patterns:
            return True
        for p in patterns:
            if p in name:
                return True
        return False

    def readScriptInfo(self, filename):
        desc = ""
        args = []
        mode = "before"

        with open(filename, "r") as f:
            for line in f:
                if mode == "before":
                    if line.startswith("##"):
                        desc = line[2:]
                        mode = "args"
                elif mode == "args":
                    if line.startswith("##"):
                        args.append(line[2:])
                    else:
                        break
        return (desc, args)

    def viewScript(self, script, arg):
        orig = self.resolveScriptName(script)
        if orig:
            mode = arg.count("v")
            if mode == 3:
                with open(orig, "r") as f:
                    sys.stdout.write(f.read())
                    sys.stdout.write("\n")
                return
            (desc, args) = self.readScriptInfo(orig)
            sys.stdout.write("{} - {}\n".format(script, desc))
            if mode == 2:
                sys.stdout.write("Arguments:\n")
                for a in args:
                    sys.stdout.write(" " + a)

    def lookupJobs(self, jobids):
        sp.call("for j in {}; do grep -w $j {}; done".format(" ".join(jobids), self.logFile), shell=True)

    def sendNotification(self, args):
        email, jobid, retcode, seconds, pwd, cmdline = args
        S = SMTP("smtp.ufl.edu")
        succ = "success" if retcode == "0" else "failed"
        body = f"""Subject: Job {jobid}: {succ}
From: {email}
To: {email}
Content-type: text/html

<!DOCTYPE html>
<HTML>
<BODY>
<PRE>
<B>Job ID:</B>      {jobid}
<B>Retcode:</B>     {retcode}
<B>Seconds:</B>     {seconds}
<B>Directory:</B>   {pwd}
<B>Commandline:</B> {cmdline}
</PRE>
</BODY>
</HTML>

"""
        S.sendmail(email, email, body)

### PBS support

class SubmitPBS(Submit):
    varNames = []

    def setVars(self):
        subargs = self.trueArgs[1:]
        self.varNames.append("args")
        os.putenv("args", " ".join(subargs))
        idx = 1
        for arg in subargs:
            name = "arg{}".format(idx)
            self.varNames.append(name)
            os.putenv(name, arg)
            idx += 1

    def makeCmdline(self, name):
        self.setVars()
        cmdline = 'qsub -d "`pwd`"'
        if self.varNames:
            cmdline += " -v " + ",".join(self.varNames)
        if self.afterArgs:
            aspecs = [ "afterok:" + a for a in self.afterArgs ]
            cmdline += " -W depend=" + ",".join(aspecs)
        if self.queue:
            cmdline += " -q " + self.queue
        if self.array:
            cmdline += " -t" + self.array
        if self.foptions:
            cmdline += " " + self.foptions
        if self.coptions:
            cmdline += " " + " ".join(self.coptions)

        return cmdline + " " + " ".join(self.trueArgs)

def getMode(arglist):
    """Returns the mode (currently one of `slurm' or `pbs') examining 
the SUBMIT_MODE environment variable and the command line (-mode option)."""
    mode = os.getenv("SUBMIT_MODE") or "slurm"
    nargs = len(arglist)
    for i in range(nargs):
        if arglist[i] == "-mode" and i < nargs - 1:
            mode = arglist[i+1]
            break
    return mode

DEFAULTS = {'slurm': {'class': Submit,
                      'command': "sbatch",
                      'conf': ".sbatchrc",
                      'directive': "#SBATCH",
                      'jobid': "${SLURM_JOBID}"},

            'pbs': {'class': SubmitPBS,
                    'command': "squeue",
                    'conf': ".qsubrc",
                    'directive': "#PBS",
                    'jobid': "${PBS_JOBID}"}}

if __name__ == "__main__":
    arglist = sys.argv[1:]
    mode = getMode(arglist)
    if mode in DEFAULTS:
        subclass = DEFAULTS[mode]['class']
        S = subclass(mode)
        if S.parseArgs(arglist):
            if S.debug > 1:
                S.dump()
                S.main()
            else:
                try:
                    S.main()
                except Exception as e:
                    sys.stderr.write("Error: {}\n".format(e))
                    sys.exit(6)
    else:
        sys.stderr.write("Error: mode should be one of {}.\n".format(", ".join(DEFAULTS.keys())))
        sys.exit(1)
