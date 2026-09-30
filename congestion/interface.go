// Package congestion ([VPP] fork) is the pluggable congestion-control API of
// github.com/apernet/quic-go, re-exported as type aliases so one controller
// implementation (VPN Portal's BBR) serves both QUIC stacks. See
// (*Conn).SetCongestionControl.
package congestion

import ap "github.com/apernet/quic-go/congestion"

type (
	ByteCount           = ap.ByteCount
	PacketNumber        = ap.PacketNumber
	AckedPacketInfo     = ap.AckedPacketInfo
	LostPacketInfo      = ap.LostPacketInfo
	CongestionControl   = ap.CongestionControl
	CongestionControlEx = ap.CongestionControlEx
	RTTStatsProvider    = ap.RTTStatsProvider
)

const (
	InitialPacketSize          = ap.InitialPacketSize
	MinPacingDelay             = ap.MinPacingDelay
	MaxPacketBufferSize        = ap.MaxPacketBufferSize
	MinInitialPacketSize       = ap.MinInitialPacketSize
	MaxCongestionWindowPackets = ap.MaxCongestionWindowPackets
	PacketsPerConnectionID     = ap.PacketsPerConnectionID
)
