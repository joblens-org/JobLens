#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    int fd = open(argv[1], O_CREAT | O_RDWR | O_TRUNC, 0600);
    if (fd < 0) return 3;
    char buf[4096] = {0};
    printf("%d\n", getpid());
    fflush(stdout);
    for (int i = 0; i < 1200; ++i) {
        struct stat st;
        if (pwrite(fd, buf, sizeof(buf), 0) != sizeof(buf) ||
            pread(fd, buf, sizeof(buf), 0) != sizeof(buf) || fsync(fd) || fstat(fd, &st)) return 4;
        int extra = open(argv[1], O_RDONLY);
        if (extra < 0 || close(extra)) return 5;
        int missing = open("/tmp/jl-bug004-fix-deliberately-missing", O_RDONLY);
        if (missing >= 0) { close(missing); return 6; }
        if (errno != ENOENT) return 7;
        usleep(100000);
    }
    return close(fd) != 0;
}
