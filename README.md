# submit.py
### Generalized submit command for slurm / moabs

The **submit.py** command submits jobs to a *slurm* or *PBS*-based schedule in a cluster environment. It is a 
user-friendly replacement for **sbatch** or **qsub**, and offers several useful features 
compared to the original commands. Its basic syntax is:

```
$ submit [submit_options] script.qsub [script_arguments...]
```

For historical reasons, we will assume that the script you are submitting has a .qsub extension, 
and we will refer to it as the qsub script. Full documentation on all options can be printed using
the `-h` or `–help` options.

Here are some of the features provided by submit, with examples of their use.

## Uniform syntax

**submit.py** unifies slurm and PBS syntax, allowing you to write job submission scripts that work 
in both environments without change. This includes passing command-line arguments to your qsub 
script under PBS, something that is not directly supported by the qsub command. In order to 
take advantage of this last feature, use variable names `$arg1,  $arg2, ...` to refer to the script’s arguments.

You can use the `-mode` command-line option to choose between the two environments, or set the `SUBMIT_MODE` 
environment variable in your shell initialization script. In both cases, allowed values are “slurm” and “pbs”.

## Output files

**submit.py** provides defaults for the files where the job’s standard output and standard error are saved. 
They will be called `scriptName.IN.oNNN` and `scriptName.IN.eNNN` respectively, where `NNN` is the job number. 
In addition, it will print the command line and the job start time at the beginning of the standard output file,
and the job end time at the end, allowing you to see what command was submitted exactly and how long its execution took.

## Job control

You can instruct **submit.py** to create a special file when the job you are submitting terminates. This is 
especially useful when your master script submits multiple jobs at the same time, and needs to wait until 
they are all are done before proceeding.  This is accomplished using the `-done` command-line option, followed 
by the name of the file to be created (by convention, this file should have extension .done). If the filename 
contains the @ character, it will be replaced with the id of the submitted job.

For example, let’s imagine we want to process three different files (file1, file2, and file3) with the `test.qsub` 
script, and we want to pause until all three jobs are complete. This is accomplished with the following code:

```bash
$ for f in file1 file2 file3; do
  submit.p -done test.@.done test.qsub $f
done
```

The above loop will submit three different instances of test.qsub, and each one will create a file called `test.NNN.done` 
when it terminates, where NNN is its job id. Then simply count the number of files of the form `test.*.done`, and wait until it reaches 3. For example, the following bash code fragment checks the number of done files every minute:

```bash
$ while true; do
  N=$(ls test.*.done | wc -l)
  if [ "$N" == "3" ]; then break; fi
  sleep 60
done
```

You can also schedule a job to be executed after a previous one terminates. This is done using the `-after` 
command-line option in the second job, followed by the id of the first one. You can take advantage of the 
fact that submit prints the job id of the submitted job when done. So, to schedule job2.qsub to run after 
job1.qsub, you can do:

```bash
$ JOB1=$(submit.py job1.qsub)
$ submit.py -after $JOB1 job2.qsub
```

## Configuration file

You can save commonly-used command-line options to a configuration file, by default `.sbatchrc` in your home 
directory (this can be changed with the `-conf` command-line option. The configuration file should contain 
`option=value` entries as you would write them on the sbatch or qsub command-line. For example, if you always 
submit your jobs to the acct1 account and you would like to receive a notification email when a job fails, 
you can put the following in your `.sbatchrc` file:

```
--account=acct1
--mail-user=<your-email-address>
--mail-type=FAIL
```

## Scripts library

If the qsub script you are submitting is not found in the current directory, it will be looked up in a
*script library.* The location of the script library can be specified with the `-lib` option or by 
setting the `SUBMIT_LIB` environment variable. This allows you to create a library of frequently-used
submission scripts that can be invoked simply by name, instead of having to specify their full path. 
You can use the `-ls` option to list all the scripts in your library, in alphabetical order.

Once you know the name of the script you want to call, you can use the `-v` option to print its usage 
message. A single `-v` will print only a description of the script, `-vv` will print its required and 
optional arguments, and `-vvv` will print the whole script. This requires the submission script to contain
a header formatted the following way:

```
#!/bin/bash

## One-line description
## arg1 = description of first argument
## arg2 = description of second argument
## ...

(rest of script)
```

## Miscellaneous options

You can use the `-o` option to pass additional options to the underlying submission program (sbatch or qsub). For example, to specify time and memory limits for the job you are submitting, you can use:

```bash
$ submit.py -o --mem=10G,--time=20:00:00 test.qsub
```

- or -

```bash
$ submit.py -o --mem=10G -o --time=20:00:00 test.qsub
```

Note that you can supply multiple directives after `-o`, in which case they should be separated by commas with no 
spaces between them, or you can supply `-o` multiple times.

The `-n` option will not modify the qsub script before submitting it. This will disable the `-done` feature, and 
will not include the start and end times in the output file.

The `-p` option allows you to associate a short comment to each submitted job. This is useful, for example, to 
distinguish jobs with the same name submitted for different projects.

The `-t` option is used to specify a job array. For example: "-t 0-15%4" will run an array of 16 jobs, numbered from
0 to 15, with the constraint that at most 4 of them will run at the same time.

The `-T` option specifies a job array based on the contents of a file. For example, if file "names.txt" contains 10
lines, "-T names.txt%4" will execute an array of 10 jobs (with the constraint that only 4 will run at the same time).
Within each job, the variable JOB_FILEARRAY_LINE will be set to the contents of one of the lines in the file.

The `-log` option allows you to specify a log file where all submitted commands are recorded. This is useful to 
compile statistics on how often each qsub script is used.

The `-x` option prints the sbatch or qsub command that would be executed, without actually executing it (useful 
for checking syntax).

## Return codes

The **submit.py** command returns one of the following return codes:

Code | Meaning
-----|--------
0    | Success
1 | Invalid mode specified (should be one of slurm or pbs)
2 | Sbatch error (e.g., incorrect submission parameters)
3 | File specified with -T does not exist
4 | Missing script name
5 | Script not found
6 | Any other error

