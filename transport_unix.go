//go:build unix

// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"os"
	"syscall"
)

func ownedPrivateDirectory(path string) bool {
	directory, e := os.Lstat(path)
	if e != nil || !directory.IsDir() || directory.Mode()&os.ModeSymlink != 0 || directory.Mode().Perm() != 0700 {
		return false
	}
	stat, ok := directory.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
