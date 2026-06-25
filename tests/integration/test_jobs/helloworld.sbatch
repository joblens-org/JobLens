#!/bin/bash
# JobLens 集成测试 — Hello World Slurm demo 作业 (预检用)
# 用法: sbatch tests/integration/test_jobs/helloworld.sbatch
# 运行时长远小于 sleep.sbatch，适合快速预检

#SBATCH --job-name=joblens_test_hw
#SBATCH --output=/tmp/hw_slurm_%j.out
#SBATCH --ntasks=1
#SBATCH --time=1:00

echo "hello world from Slurm"
