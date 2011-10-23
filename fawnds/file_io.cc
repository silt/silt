#include "file_io.h"

#ifdef __APPLE__
namespace fawn {

	// non-atomic emulation of preadv and pwritev

	ssize_t
	preadv(int fd, const struct iovec *iovec, int count, off_t offset)
	{
		ssize_t total_read_bytes = 0;
		for (int i = 0; i < count; i++) {
			ssize_t read_bytes = pread(fd, iovec[i].iov_base, iovec[i].iov_len, offset);
			if (read_bytes < 0)
				return read_bytes;
			if (static_cast<size_t>(read_bytes) < iovec[i].iov_len)
				break;
			total_read_bytes += read_bytes;
			offset += iovec[i].iov_len;
		}
		return total_read_bytes;
	}

	ssize_t
	pwritev(int fd, const struct iovec *iovec, int count, off_t offset)
	{
		ssize_t total_written_bytes = 0;
		for (int i = 0; i < count; i++) {
			ssize_t written_bytes = pwrite(fd, iovec[i].iov_base, iovec[i].iov_len, offset);
			if (written_bytes < 0)
				return written_bytes;
			if (static_cast<size_t>(written_bytes) < iovec[i].iov_len)
				break;
			total_written_bytes += written_bytes;
			offset += iovec[i].iov_len;
		}
		return total_written_bytes;
	}

}
#endif

