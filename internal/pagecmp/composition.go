package pagecmp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/klauspost/compress/zstd"

	"github.com/CloudnetUCSC/statepod/internal/pipeline"
)

// Composition answers RQ1 for one dump : of the memory a
// migration must carry, how much can the destination supply from its
// synthesized baseline (B), how much is zero, and how much is unique to the
// workload, broken down by the kind of memory it lives in. Unique pages are
// also compressed (zstd level 1, per page, as the pipeline does) to give the
// compressibility factor of the bytes model: bytes ~ unique * c + refs.
type Composition struct {
	DumpA   string                  `json:"dump_a"`
	DumpB   string                  `json:"dump_b"`
	Regions map[string]*RegionStats `json:"regions"`
	Total   RegionStats             `json:"total"`
}

// RegionStats counts the pages of one memory region. A page is exactly one of
// Zero (all zero bytes), Hit (its content exists in the baseline: a manifest
// hit) or Unique. Hit takes precedence over Zero only when the baseline holds
// non-zero content for it, which cannot happen: zero pages are reported as Zero.
type RegionStats struct {
	Pages       int     `json:"pages"`
	Zero        int     `json:"zero"`
	Hit         int     `json:"hit"`
	Unique      int     `json:"unique"`
	UniqueBytes int64   `json:"unique_bytes"`
	UniqueZstd  int64   `json:"unique_zstd_bytes"`
	HitRate     float64 `json:"hit_rate"`   // Hit / Pages (zero pages excluded from hits)
	ZstdRatio   float64 `json:"zstd_ratio"` // UniqueZstd / UniqueBytes
}

func (r *RegionStats) finish() {
	if r.Pages > 0 {
		r.HitRate = float64(r.Hit) / float64(r.Pages)
	}
	if r.UniqueBytes > 0 {
		r.ZstdRatio = float64(r.UniqueZstd) / float64(r.UniqueBytes)
	}
}

// Compose breaks dump A down against baseline dump B (B may be "" for no
// baseline: then every non-zero page is Unique). vmaDir holds the mm-<pid>.img
// VMA lists used to name regions; "" = dirA. A pre-dump directory has no mm
// images, so point vmaDir at a full dump of the same processes.
func Compose(dirA, dirB, vmaDir string) (*Composition, error) {
	if vmaDir == "" {
		vmaDir = dirA
	}
	base := map[[32]byte]bool{}
	if dirB != "" {
		b, err := LoadDump(dirB)
		if err != nil {
			return nil, fmt.Errorf("loading baseline %s: %w", dirB, err)
		}
		for h := range b.Hashes {
			base[h] = true
		}
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	defer enc.Close()

	c := &Composition{DumpA: dirA, DumpB: dirB, Regions: map[string]*RegionStats{}}
	vmas := map[int][]pipeline.VMA{}
	region := func(pid int, vaddr uint64) string {
		if pid < 0 {
			return "shared" // pagemap-shmem-<id>: a shared-memory segment
		}
		vs, ok := vmas[pid]
		if !ok {
			vs, _ = pipeline.ReadVMAs(vmaDir, pid) // missing mm image: pages count as "other"
			sort.Slice(vs, func(i, j int) bool { return vs[i].Start < vs[j].Start })
			vmas[pid] = vs
		}
		i := sort.Search(len(vs), func(i int) bool { return vs[i].End > vaddr })
		if i < len(vs) && vaddr >= vs[i].Start {
			return vs[i].Region()
		}
		return "other"
	}
	err = walkDump(dirA, func(pid int, vaddr uint64, page []byte) {
		name := region(pid, vaddr)
		r := c.Regions[name]
		if r == nil {
			r = &RegionStats{}
			c.Regions[name] = r
		}
		r.Pages++
		h := sha256.Sum256(page)
		switch {
		case h == zeroPageHash:
			r.Zero++
		case base[h]:
			r.Hit++
		default:
			r.Unique++
			r.UniqueBytes += int64(len(page))
			r.UniqueZstd += int64(len(enc.EncodeAll(page, nil)))
		}
	})
	if err != nil {
		return nil, err
	}
	for _, r := range c.Regions {
		r.finish()
		c.Total.Pages += r.Pages
		c.Total.Zero += r.Zero
		c.Total.Hit += r.Hit
		c.Total.Unique += r.Unique
		c.Total.UniqueBytes += r.UniqueBytes
		c.Total.UniqueZstd += r.UniqueZstd
	}
	c.Total.finish()
	return c, nil
}

// JSON renders the composition for the analysis scripts.
func (c *Composition) JSON() string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	_ = enc.Encode(c)
	return buf.String()
}
