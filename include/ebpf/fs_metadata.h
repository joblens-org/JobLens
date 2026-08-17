/* Copyright 2026 - 2026 wzycc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
#ifndef JOB_FS_METADATA_STAT
#define JOB_FS_METADATA_STAT
// 用于给用户态程序共享数据结构
// 注意：在 .bpf.c 文件中，此头文件应放在 vmlinux.h 和 bpf_helpers.h 之后包含
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

/* 文件系统元数据操作类型枚举 - 稳定ABI，显式整数值 */
enum fs_meta_op {
    FS_META_OPEN        = 0,   /* 打开文件 */
    FS_META_CLOSE       = 1,   /* 关闭文件 */
    FS_META_GETATTR     = 2,   /* 获取文件属性 */
    FS_META_READDIR     = 3,   /* 读取目录 */
    FS_META_CREATE      = 4,   /* 创建文件 */
    FS_META_MKDIR       = 5,   /* 创建目录 */
    FS_META_MKNOD       = 6,   /* 创建特殊文件 */
    FS_META_UNLINK      = 7,   /* 删除文件 */
    FS_META_RMDIR       = 8,   /* 删除目录 */
    FS_META_RENAME      = 9,   /* 重命名文件/目录 */
    FS_META_LINK        = 10,  /* 创建硬链接 */
    FS_META_SYMLINK     = 11,  /* 创建符号链接 */
    FS_META_READLINK    = 12,  /* 读取符号链接 */
    FS_META_SETATTR     = 13,  /* 设置文件属性 */
    FS_META_GETXATTR    = 14,  /* 获取扩展属性 */
    FS_META_SETXATTR    = 15,  /* 设置扩展属性 */
    FS_META_LISTXATTR   = 16,  /* 列出扩展属性 */
    FS_META_REMOVEXATTR = 17,  /* 删除扩展属性 */
    FS_META_STATFS      = 18,  /* 获取文件系统统计信息 */
    FS_META_SYNC        = 19,  /* 同步文件系统 */
    FS_META_FSYNC       = 20,  /* 同步文件数据 */
    FS_META_MAX         = 21,  /* 操作类型最大值（用于边界检查） */
};

/* 累加器 map 键：进程ID + 操作类型 */
struct fs_meta_key {
    u32 pid;    /* 进程ID */
    u32 op;     /* 操作类型，enum fs_meta_op 转换为 u32 */
};

/* 累加器 map 值：文件系统元数据统计信息 */
struct fs_meta_stat {
    u64 calls;              /* 总调用次数 */
    u64 success;            /* 成功次数 */
    u64 errors;             /* 错误次数 */
    s64 last_errno;         /* 最后一次错误码（负数） */
    u64 total_latency_ns;   /* 总延迟（纳秒） */
    u64 max_latency_ns;     /* 最大延迟（纳秒） */
    u64 last_timestamp_ns;  /* 最后一次操作时间戳（纳秒） */
};

#endif
