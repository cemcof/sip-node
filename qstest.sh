#!/bin/bash
### Inherit all current environment variables
#PBS -V
### Job name
#PBS -N testjob
### Queue name
#PBS -q default
### Standard output and standard error messages PBS -k eo
### Specify the number of nodes and thread (ppn) for your job.
#PBS -l select=1:ncpus=4:ngpus=2:mem=62gb:scratch_local=1000gb
### Tell PBS the anticipated run-time for your job, where walltime=HH:MM:SS
#PBS -l walltime=72:00:00
# Use as working dir the path where qsub was launched

PATH=$PBS_O_PATH

which python3
env