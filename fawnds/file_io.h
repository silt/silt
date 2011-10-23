/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FILE_IO_H_
#define _FILE_IO_H_

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif  // #ifndef _LARGEFILE64_SOURCE

#define _FILE_OFFSET_BITS 64

#ifndef O_NOATIME
#define O_NOATIME 0  /* O_NOATIME is linux-only */
#endif

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600   // for posix_fadvise()
#endif
#ifndef __USE_BSD
#define __USE_BSD           // for preadv() and pwritev()
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

#ifdef __APPLE__
#define pread64 pread
#define pwrite64 pwrite
namespace fawn {
    ssize_t preadv(int fd, const struct iovec *iovec, int count, off_t offset);
    ssize_t pwritev(int fd, const struct iovec *iovec, int count, off_t offset);
}  // namespace fawn
#endif // #ifdef __APPLE__

#endif // #ifndef _FILE_IO_H_
