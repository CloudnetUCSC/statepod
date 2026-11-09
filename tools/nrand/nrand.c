/*
 * nrand — exec a command with ASLR disabled (ADDR_NO_RANDOMIZE).
 *
 * Deterministic-launch entrypoint wrapper for statepod bundles :
 * personality() persists across execve, so the workload and everything it
 * spawns run without address-space randomization. Built statically so the
 * same binary works inside any container rootfs (musl, glibc, none).
 */
#include <sys/personality.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	if (argc < 2) {
		fprintf(stderr, "usage: nrand cmd [args...]\n");
		return 2;
	}

	int cur = personality(0xffffffff);
	if (cur == -1 || personality(cur | ADDR_NO_RANDOMIZE) == -1) {
		perror("nrand: personality");
		return 1;
	}

	execvp(argv[1], &argv[1]);
	perror("nrand: execvp");
	return 127;
}
