package migration

import (
	"regexp"
	"strconv"
	"strings"
)

var rsyncSentBytesRE = regexp.MustCompile(`sent ([\d,]+) bytes`)

// parseRsyncSentBytes extracts the "sent X bytes" figure from rsync --stats
// output (callers force LC_ALL=C so the thousands separator is always ",").
// Used by the precopy-rsync/sync-rsync transport A/B (addendum) to
// report a wire-bytes figure comparable to the tar+custom-TCP transport's
// directory-size accounting.
func parseRsyncSentBytes(statsOutput string) int64 {
	m := rsyncSentBytesRE.FindStringSubmatch(statsOutput)
	if m == nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.ReplaceAll(m[1], ",", ""), 10, 64)
	return n
}
