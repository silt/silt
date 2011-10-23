#include "fawnds_factory.h"
#include <cstdlib>
#include <cassert>
#include <vector>
#include <cstdio>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>

void benchmark()
{
    fprintf(stdout, "# number_of_files write_speed_MBps\n");

	for (size_t i = 1; i <= 256; i *= 2)
	{
		for (size_t j = 0; j < i; j++) {
			char buf[1024];
			sprintf(buf, "file%zu", j);

			unlink(buf);
        }
        sync();

		fprintf(stdout, "%zu ", i);
		fflush(stdout);

		std::vector<int> fds;
		for (size_t j = 0; j < i; j++) {
			char buf[1024];
			sprintf(buf, "file%zu", j);

			int fd = open(buf, O_WRONLY | O_CREAT | O_NOATIME, 0666);
			if (fd == -1) {
				perror("");
				return;
			}
			fds.push_back(fd);
		}

		srand(time(NULL));

		struct timeval tv;
		uint64_t start_time, end_time;

		const int64_t total_bytes_to_write = 24L * 1000 * 1000 * 1000;
		int64_t remaining_bytes_to_write = total_bytes_to_write;

		gettimeofday(&tv, NULL);
		start_time = tv.tv_sec * 1000000L + tv.tv_usec;

		char buf[4096];
		while (remaining_bytes_to_write >= 0) {
            uint32_t r = rand();
			for (size_t j = 0; j < sizeof(buf); j += 4)
				*reinterpret_cast<uint32_t*>(buf + j) = r++;

			size_t fd_idx = rand() % i;
			if (write(fds[fd_idx], buf, sizeof(buf)) != sizeof(buf)) {
				perror("");
				return;
			}

			remaining_bytes_to_write -= sizeof(buf);
		}

		gettimeofday(&tv, NULL);
		end_time = tv.tv_sec * 1000000L + tv.tv_usec;

		fprintf(stdout, "%lf\n", (double)total_bytes_to_write / 1000000. / (double)(end_time - start_time) * 1000000L);

		for (size_t j = 0; j < i; j++)
			close(fds[j]);
	}
}


int main(int argc, char** argv) {
	benchmark();
	return 0;
}

