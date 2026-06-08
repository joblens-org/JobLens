#!/bin/sh
# %post scriptlet for joblens RPM
# $1 = 1 (首次安装) / 2 (升级)

rm -f /var/JobLens/JobLens.lock

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
