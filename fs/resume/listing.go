package resume

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/lib/transform"
	"golang.org/x/text/unicode/norm"
)

type resumableDirLister interface {
	ResumeListDir(ctx context.Context, dir, continuationToken string) (entries fs.DirEntries, nextContinuationToken string, tokenAccepted bool, err error)
}

// DirPage is a filtered page of directory entries for the resume scanner.
type DirPage struct {
	ContinuationToken     string
	Entries               fs.DirEntries
	NextContinuationToken string
	TokenAccepted         bool
	NativeOrder           bool
}

// MatchKeyer reproduces the source/destination comparison keys used by march.
type MatchKeyer struct {
	ctx        context.Context
	transforms []func(string) string
}

// NewMatchKeyer returns a keyer compatible with fs/march sorting semantics.
func NewMatchKeyer(ctx context.Context, fdst fs.Fs) MatchKeyer {
	ci := fs.GetConfig(ctx)
	keyer := MatchKeyer{ctx: ctx}
	if !ci.NoUnicodeNormalization {
		keyer.transforms = append(keyer.transforms, norm.NFC.String)
	}
	if fdst.Features().CaseInsensitive || ci.IgnoreCaseSync {
		keyer.transforms = append(keyer.transforms, strings.ToLower)
	}
	return keyer
}

// SrcKey builds the matching key for a source directory entry.
func (k MatchKeyer) SrcKey(entry fs.DirEntry) string {
	return k.entryKey(entry, true)
}

// DstKey builds the matching key for a destination directory entry.
func (k MatchKeyer) DstKey(entry fs.DirEntry) string {
	return k.entryKey(entry, false)
}

func (k MatchKeyer) entryKey(entry fs.DirEntry, isSrc bool) string {
	if entry == nil {
		return ""
	}
	name := path.Base(entry.Remote())
	_, isDirectory := entry.(fs.Directory)
	if isSrc {
		name = transform.Path(k.ctx, name, isDirectory)
	}
	for _, transformFn := range k.transforms {
		name = transformFn(name)
	}
	if isDirectory {
		name += "D"
	} else {
		name += "F"
	}
	return name
}

// TargetRemote returns the transformed destination remote for the source entry.
func TargetRemote(ctx context.Context, entry fs.DirEntry) string {
	_, isDir := entry.(fs.Directory)
	return transform.Path(ctx, entry.Remote(), isDir)
}

// ListDirPage lists a directory page for resume-aware traversal. Backends with
// a native resume cursor may return a partial page in native listing order;
// other backends return the full sorted directory.
func ListDirPage(ctx context.Context, f fs.Fs, dir string, includeAll bool, keyFn func(fs.DirEntry) string, continuationToken string) (page DirPage, err error) {
	if lister, ok := f.(resumableDirLister); ok {
		fi := filter.GetConfig(ctx)
		if !includeAll {
			excluded, err := fi.DirContainsExcludeFile(ctx, f, dir)
			if err != nil {
				return DirPage{}, err
			}
			if excluded {
				fs.Debugf(dir, "Excluded")
				return DirPage{TokenAccepted: true, NativeOrder: true}, nil
			}
		}
		page.Entries, page.NextContinuationToken, page.TokenAccepted, err = lister.ResumeListDir(ctx, dir, continuationToken)
		accounting.Stats(ctx).Listed(int64(len(page.Entries)))
		if err != nil {
			return DirPage{}, err
		}
		page.NativeOrder = true
		if page.TokenAccepted {
			page.ContinuationToken = continuationToken
		}
		page.Entries, err = filterEntries(ctx, f, dir, page.Entries, includeAll)
		if err != nil {
			return DirPage{}, err
		}
		return page, nil
	}

	entries, err := SortedDirEntries(ctx, f, dir, includeAll, keyFn)
	if err != nil {
		return DirPage{}, err
	}
	return DirPage{
		Entries:           entries,
		TokenAccepted:     true,
		ContinuationToken: "",
	}, nil
}

// CursorKey returns the persisted cursor key for an entry at the supplied
// logical page and index. This makes resume boundaries unique even when
// multiple entries compare equal under case folding or Unicode normalization.
func CursorKey(pageIndex, entryIndex int) string {
	return fmt.Sprintf("%08x:%08x", pageIndex, entryIndex)
}

// SortedDirEntries lists, filters, and sorts a directory using the supplied key
// function while updating the standard listed counter.
func SortedDirEntries(ctx context.Context, f fs.Fs, dir string, includeAll bool, keyFn func(fs.DirEntry) string) (fs.DirEntries, error) {
	entries, err := f.List(ctx, dir)
	accounting.Stats(ctx).Listed(int64(len(entries)))
	if err != nil {
		return nil, err
	}
	fi := filter.GetConfig(ctx)
	if !includeAll && fi.ListContainsExcludeFile(entries) {
		fs.Debugf(dir, "Excluded")
		return nil, nil
	}
	entries, err = filterEntries(ctx, f, dir, entries, includeAll)
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(entries, func(a, b fs.DirEntry) int {
		return cmp.Compare(keyFn(a), keyFn(b))
	})
	return entries, nil
}

func filterEntries(ctx context.Context, f fs.Fs, dir string, entries fs.DirEntries, includeAll bool) (fs.DirEntries, error) {
	fi := filter.GetConfig(ctx)
	newEntries := entries[:0]
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	for _, entry := range entries {
		ok := true
		switch x := entry.(type) {
		case fs.Object:
			if !includeAll && !fi.IncludeObject(ctx, x) {
				ok = false
				fs.Debugf(x, "Excluded")
			}
		case fs.Directory:
			if !includeAll {
				include, err := fi.IncludeDirectory(ctx, f)(x.Remote())
				if err != nil {
					return nil, err
				}
				if !include {
					ok = false
					fs.Debugf(x, "Excluded")
				}
			}
		default:
			return nil, fs.ErrorNotImplemented
		}
		remote := entry.Remote()
		switch {
		case !ok:
		case !strings.HasPrefix(remote, prefix):
			ok = false
			fs.Errorf(entry, "Entry doesn't belong in directory %q (too short) - ignoring", dir)
		case remote == dir:
			ok = false
			fs.Errorf(entry, "Entry doesn't belong in directory %q (same as directory) - ignoring", dir)
		case strings.ContainsRune(remote[len(prefix):], '/'):
			ok = false
			fs.Errorf(entry, "Entry doesn't belong in directory %q (contains subdir) - ignoring", dir)
		}
		if ok {
			newEntries = append(newEntries, entry)
		}
	}
	return newEntries, nil
}

// DirByKey indexes the first entry for each destination comparison key.
func DirByKey(entries fs.DirEntries, keyFn func(fs.DirEntry) string) map[string]fs.DirEntry {
	out := make(map[string]fs.DirEntry, len(entries))
	for _, entry := range entries {
		key := keyFn(entry)
		if _, ok := out[key]; !ok {
			out[key] = entry
		}
	}
	return out
}

// EntryIdentity returns a stable identity for journaling destination entries
// already paired during check source scanning.
func EntryIdentity(ctx context.Context, entry fs.DirEntry) string {
	if entry == nil {
		return ""
	}
	switch x := entry.(type) {
	case fs.ObjectInfo:
		return "obj:" + x.Remote() + ":" + fs.Fingerprint(ctx, x, true)
	case fs.Directory:
		return "dir:" + x.Remote()
	default:
		return entry.Remote()
	}
}

// Supported reports whether resume scanning is available for the filesystem.
func Supported(f fs.Fs) bool {
	return f != nil
}
