package resume

import (
	"context"
	"fmt"

	"github.com/rclone/rclone/fs"
)

// WorkKey returns the persisted identity of a work item.
func WorkKey(op, srcRemote, dstRemote, fingerprint string) string {
	return hashString(fmt.Sprintf("%s|%s|%s|%s", op, srcRemote, dstRemote, fingerprint))
}

// EntryFingerprint returns the stable fingerprint for a filesystem entry.
func EntryFingerprint(ctx context.Context, entry fs.DirEntry) string {
	if entry == nil {
		return ""
	}
	if objectInfo, ok := entry.(fs.ObjectInfo); ok {
		return fs.Fingerprint(ctx, objectInfo, true)
	}
	return "dir:" + entry.Remote()
}
