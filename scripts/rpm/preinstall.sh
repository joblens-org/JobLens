#!/bin/sh
# %pre scriptlet for joblens RPM
# $1 = 1 (首次安装) / 2 (升级)
#
# 目的：升级时将现有配置文件移出 RPM 视线，
# 防止 RPM 因数据库记录"未修改"而覆盖用户配置。
# %post 脚本会将备份恢复（如内容不同则保留旧版）。

if [ "$1" -eq 2 ] && [ -f /etc/JobLens/config.yaml ]; then
    mv /etc/JobLens/config.yaml /etc/JobLens/config.yaml.rpmorig
fi

exit 0
