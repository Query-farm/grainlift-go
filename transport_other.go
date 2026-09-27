//go:build !unix

// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

func ownedPrivateDirectory(string) bool { return false }
