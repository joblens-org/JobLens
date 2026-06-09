#!/bin/sh
# %preun scriptlet for joblens RPM
# $1 = 0 (完全卸载) / 1 (升级)

if [ -x /usr/bin/systemctl ]; then
    if [ "$1" -eq 0 ]; then
        # 完全卸载：先 disable（下次开机不再启动），再停止当前实例
        /usr/bin/systemctl --no-reload disable joblens.service >/dev/null 2>&1 || :
        /usr/bin/systemctl stop joblens.service >/dev/null 2>&1 || :
    elif [ "$1" -eq 1 ]; then
        # 升级：停止旧版本，%post 中 try-restart 将启动新版本
        /usr/bin/systemctl stop joblens.service >/dev/null 2>&1 || :
    fi
fi

exit 0
