// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Package grainlift implements the Grainlift 0.4 server-side ADBC contract.
// Clients use the ordinary native Grainlift ADBC driver. Backend callbacks own
// database behavior; Service owns authenticated handles and bounded Arrow cursors.
package grainlift
