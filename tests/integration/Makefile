# JobLens 集成测试 Makefile
# 所有目标从 tests/integration/ 目录执行
SHELL := /bin/bash
VENV := .venv
PYTEST := $(VENV)/bin/pytest

.PHONY: help up venv provision test test-condor test-slurm logs clean preset preset-list preset-match

help: ## 显示帮助信息
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

up: ## 启动 Vagrant VM (libvirt)
	vagrant up --provider=libvirt

venv: ## 创建 Python 虚拟环境并安装依赖
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

provision: ## 部署所有服务 (common + condor + slurm + joblens)
	vagrant ssh controller -c "sudo bash /vagrant/provisioning/common.sh controller"
	vagrant ssh worker -c "sudo bash /vagrant/provisioning/common.sh worker"
	vagrant ssh controller -c "sudo bash /vagrant/provisioning/condor/controller.sh"
	vagrant ssh worker -c "sudo bash /vagrant/provisioning/condor/worker.sh"
	vagrant ssh controller -c "sudo bash /vagrant/provisioning/slurm/controller.sh"
	# 从 controller 提取 slurm 运行时文件（Vagrant synced_folder 不保证 guest→host 同步）
	mkdir -p .runtime/slurm
	vagrant ssh controller -c "sudo cat /etc/munge/munge.key" > .runtime/slurm/munge.key
	vagrant ssh controller -c "sudo cat /etc/slurm/slurm.conf" > .runtime/slurm/slurm.conf
	# 注入到 worker（先创建目录再传输文件）
	vagrant ssh worker -c "sudo mkdir -p /var/tmp/slurm_runtime"
	vagrant ssh worker -c "sudo tee /var/tmp/slurm_runtime/munge.key" < .runtime/slurm/munge.key > /dev/null
	vagrant ssh worker -c "sudo tee /var/tmp/slurm_runtime/slurm.conf" < .runtime/slurm/slurm.conf > /dev/null
	vagrant ssh worker -c "sudo bash /vagrant/provisioning/slurm/worker.sh"
	vagrant ssh worker -c "sudo bash /vagrant/provisioning/joblens/deploy.sh"

test: venv ## 运行全部集成测试 (alm9-default 预设)
	python3 ./run_preset.py alm9-default

test-condor: venv ## 仅 HTCondor 发现测试 (4 个)
	$(PYTEST) -v --tb=short test_02_htcondor_discovery.py

test-slurm: venv ## 仅 Slurm 发现测试 (4 个)
	$(PYTEST) -v --tb=short test_03_slurm_discovery.py

preset: venv ## 运行指定预设 (make preset NAME=alm9-default)
	python3 ./run_preset.py $(NAME)

preset-list: ## 列出所有可用预设
	python3 ./run_preset.py --list

preset-match: ## glob 匹配预设 (make preset-match PATTERN="alm9-*")
	python3 ./run_preset.py --match $(PATTERN)

logs: ## 查看 JobLens 服务日志
	vagrant ssh worker -c "sudo journalctl -u joblens --no-pager -n 50"
	@echo "==="
	vagrant ssh worker -c "sudo journalctl -u joblens-trigger --no-pager -n 30"

clean: ## 销毁 VM 并清理缓存
	-vagrant destroy -f
	rm -rf $(VENV) __pycache__ .pytest_cache .vagrant .runtime
