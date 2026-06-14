#!/bin/sh
# %post scriptlet for joblens RPM
# $1 = 1 (首次安装) / 2 (升级)

rm -f /var/JobLens/JobLens.lock

# ---- 升级时恢复配置备份 ----
# %pre 脚本将旧配置重命名为 config.yaml.rpmorig，使 RPM 无法根据数据库
# 记录判定"未修改"而覆盖。此处将备份恢复，确保升级不会丢失用户配置。
if [ "$1" -eq 2 ] && [ -f /etc/JobLens/config.yaml.rpmorig ]; then
    if cmp -s /etc/JobLens/config.yaml.rpmorig /etc/JobLens/config.yaml 2>/dev/null; then
        # 内容相同，删除备份即可
        rm -f /etc/JobLens/config.yaml.rpmorig
    else
        # 内容不同（用户修改过），保留旧配置，新版本存为 .rpmnew
        mv /etc/JobLens/config.yaml /etc/JobLens/config.yaml.rpmnew
        mv /etc/JobLens/config.yaml.rpmorig /etc/JobLens/config.yaml
        echo "info: existing config preserved at /etc/JobLens/config.yaml"
        echo "info: new default saved as /etc/JobLens/config.yaml.rpmnew"
    fi
fi

if [ -x /usr/bin/systemctl ]; then
    /usr/bin/systemctl daemon-reload >/dev/null 2>&1 || :
fi

if [ "$1" -eq 1 ] && [ -x /usr/bin/systemctl ]; then
    # 首次安装：preset 启用服务（由 preset 文件决定是否 auto-start）
    /usr/bin/systemctl preset joblens.service >/dev/null 2>&1 || :
elif [ "$1" -eq 2 ] && [ -x /usr/bin/systemctl ]; then
    # 升级：尝试重启服务
    /usr/bin/systemctl try-restart joblens.service >/dev/null 2>&1 || :
fi

exit 0
