#!/bin/sh
# %preun scriptlet for joblens RPM
# $1 = 0 (完全卸载) / 1 (升级)

if [ "$1" -eq 0 ] && [ -x /usr/bin/systemctl ]; then
    /usr/bin/systemctl --no-reload disable joblens.service >/dev/null 2>&1 || :
    /usr/bin/systemctl stop joblens.service >/dev/null 2>&1 || :
fi

exit 0
