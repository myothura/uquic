// Package monotime is, in this fork ([VPP]), an alias of the public monotime
// package of github.com/apernet/quic-go: one epoch for both QUIC stacks, so a
// congestion controller built for apernet's congestion.CongestionControl
// (VPN Portal's BBR, from Xray-core's Hysteria port) can be plugged into a
// uquic connection unchanged (see ../../congestion).
package monotime

import (
	"time"

	ap "github.com/apernet/quic-go/monotime"
)

type Time = ap.Time

func Now() Time                  { return ap.Now() }
func Since(t Time) time.Duration { return ap.Since(t) }
func Until(t Time) time.Duration { return ap.Until(t) }
func FromTime(t time.Time) Time  { return ap.FromTime(t) }
