// Package pipeline is the M1+M2 reconciliation engine (§3–4): it
// turns a source CRIU dump into a compact delta against a destination-held
// synthesized baseline (manifest of page hashes), and reassembles a
// CRIU-restorable dump on the destination from local baseline pages + the
// received delta. The guiding principle is the design's principle — never transfer
// state the destination can synthesize itself.
package pipeline

import "github.com/zeebo/blake3"

// PageSize is CRIU's page granularity on x86-64.
const PageSize = 4096

// HashLen is the manifest/ref hash width in bytes: blake3 truncated to 128
// bits. 16 B per 4 KiB page ≈ 0.39% of baseline size. Collision
// probability for 10^7 pages ≈ 2^-82 — no cross-host byte-verify needed.
const HashLen = 16

// PageHash is a blake3-128 content hash of a single 4 KiB page.
type PageHash [HashLen]byte

// HashPage returns the blake3-128 hash of a page. p need not be exactly
// PageSize (the final page of a synthetic test may be short), but in the CRIU
// path every page is exactly PageSize.
func HashPage(p []byte) PageHash {
	sum := blake3.Sum256(p) // full 32 B digest; take the low 16
	var h PageHash
	copy(h[:], sum[:HashLen])
	return h
}
