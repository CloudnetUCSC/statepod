package pipeline

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// manifestMagic tags a serialized manifest ("StatePod ManiFest").
var manifestMagic = [4]byte{'S', 'P', 'M', 'F'}

// ManifestVersion is bumped on any wire-format change so a source and
// destination on mismatched builds fail loudly instead of misreconciling.
const ManifestVersion = 1

// Manifest is the only pre-burst WAN payload: the set of
// blake3-128 hashes of every page in the destination-synthesized baseline,
// plus the image digest and a baseline id it is bound to. The source diffs its
// dump against this set — hits become 16 B refs, misses raw pages. Zero
// baseline pages ever cross the wire.
type Manifest struct {
	ImageDigest    string
	BaselineDumpID string
	Hashes         map[PageHash]struct{}
}

// Has reports whether a page hash is present in the baseline.
func (m *Manifest) Has(h PageHash) bool {
	_, ok := m.Hashes[h]
	return ok
}

// sortHashes orders hashes ascending by bytes — so re-synthesized baselines
// produce delta-friendly manifests (§3.3) and serialization /
// baseline ids are deterministic.
func sortHashes(hs []PageHash) {
	sort.Slice(hs, func(i, j int) bool { return bytes.Compare(hs[i][:], hs[j][:]) < 0 })
}

// sortedHashes returns the manifest hashes in ascending byte order.
func (m *Manifest) sortedHashes() []PageHash {
	hs := make([]PageHash, 0, len(m.Hashes))
	for h := range m.Hashes {
		hs = append(hs, h)
	}
	sortHashes(hs)
	return hs
}

// Encode writes the manifest in its binary wire form:
//
//	magic[4] version[1]
//	imageDigestLen[u16] imageDigest
//	baselineDumpIDLen[u16] baselineDumpID
//	count[u32] count*hash[16]  (hashes ascending)
func (m *Manifest) Encode(w io.Writer) error {
	if _, err := w.Write(manifestMagic[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{ManifestVersion}); err != nil {
		return err
	}
	if err := writeString16(w, m.ImageDigest); err != nil {
		return err
	}
	if err := writeString16(w, m.BaselineDumpID); err != nil {
		return err
	}
	hs := m.sortedHashes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(hs))); err != nil {
		return err
	}
	for _, h := range hs {
		if _, err := w.Write(h[:]); err != nil {
			return err
		}
	}
	return nil
}

// DecodeManifest reads a manifest written by Encode.
func DecodeManifest(r io.Reader) (*Manifest, error) {
	br := bufioReader(r)
	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("reading manifest magic: %w", err)
	}
	if magic != manifestMagic {
		return nil, fmt.Errorf("bad manifest magic %v", magic)
	}
	var ver [1]byte
	if _, err := io.ReadFull(br, ver[:]); err != nil {
		return nil, err
	}
	if ver[0] != ManifestVersion {
		return nil, fmt.Errorf("manifest version %d unsupported (want %d)", ver[0], ManifestVersion)
	}
	digest, err := readString16(br)
	if err != nil {
		return nil, err
	}
	dumpID, err := readString16(br)
	if err != nil {
		return nil, err
	}
	var count uint32
	if err := binary.Read(br, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	m := &Manifest{ImageDigest: digest, BaselineDumpID: dumpID, Hashes: make(map[PageHash]struct{}, count)}
	for i := uint32(0); i < count; i++ {
		var h PageHash
		if _, err := io.ReadFull(br, h[:]); err != nil {
			return nil, fmt.Errorf("reading manifest hash %d/%d: %w", i, count, err)
		}
		m.Hashes[h] = struct{}{}
	}
	return m, nil
}

func writeString16(w io.Writer, s string) error {
	if len(s) > 0xffff {
		return fmt.Errorf("string too long for u16 length prefix: %d", len(s))
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readString16(r io.Reader) (string, error) {
	var n uint16
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}
