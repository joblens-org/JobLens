#!/usr/bin/env bash
# JobLens 集成测试 — 最小 Slurm sbatch sleep 作业模板
# 用法: sbatch tests/integration/test_jobs/sleep.sbatch
# 必须在 controller VM (192.168.56.10) 上执行

#SBATCH --job-name=joblens_test_sleep
#SBATCH --output=/tmp/sleep_slurm_%j.out
#SBATCH --error=/tmp/sleep_slurm_%j.err
#SBATCH --ntasks=1
#SBATCH --time=2:00
#SBATCH --mem=64M

/usr/bin/sleep 60
