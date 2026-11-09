# The synthetic workload image: just the static binary. No libc, shell or
# allocator beyond what is statically linked, so the only differences between
# runs are the knobs on the command line.
FROM scratch
COPY synth_workload /synth_workload
ENTRYPOINT ["/synth_workload"]
