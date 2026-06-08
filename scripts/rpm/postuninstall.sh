#!/bin/sh
# %postun scriptlet for joblens RPM
# $1 = 0 (完全卸载) / 1 (升级)

if [ "$1" -eq 0 ] && [ -x /usr/bin/systemctl ]; then
    /usr/bin/systemctl daemon-reload >/dev/null 2>&1 || :
fi

exit 0
