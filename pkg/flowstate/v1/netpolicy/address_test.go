package netpolicy

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_classify(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want category
	}{
		{name: "public IPv4", addr: "93.184.216.34", want: catPublic},
		{name: "public IPv6", addr: "2606:2800:220:1:248:1893:25c8:1946", want: catPublic},

		{name: "IPv4 loopback", addr: "127.0.0.1", want: catLoopback},
		{name: "IPv4 loopback range", addr: "127.13.37.1", want: catLoopback},
		{name: "IPv6 loopback", addr: "::1", want: catLoopback},

		{name: "RFC 1918 ten", addr: "10.0.0.1", want: catPrivate},
		{name: "RFC 1918 172", addr: "172.16.5.4", want: catPrivate},
		{name: "RFC 1918 192", addr: "192.168.1.1", want: catPrivate},
		{name: "IPv6 unique local", addr: "fc00::1", want: catUniqueLocal},
		{name: "carrier-grade NAT", addr: "100.64.0.1", want: catCarrierGrade},

		{name: "IPv4 link-local", addr: "169.254.1.1", want: catLinkLocal},
		{name: "IPv6 link-local", addr: "fe80::1", want: catLinkLocal},

		{name: "IPv4 multicast", addr: "224.0.0.1", want: catMulticast},
		{name: "IPv6 link-local multicast", addr: "ff02::1", want: catMulticast},
		{name: "IPv6 interface-local multicast", addr: "ff01::1", want: catMulticast},

		{name: "IPv4 unspecified", addr: "0.0.0.0", want: catUnspecified},
		{name: "IPv4 this network", addr: "0.1.2.3", want: catUnspecified},
		{name: "IPv6 unspecified", addr: "::", want: catUnspecified},
		{name: "IPv4 broadcast", addr: "255.255.255.255", want: catBroadcast},

		{name: "AWS and GCP metadata", addr: "169.254.169.254", want: catMetadata},
		{name: "AWS ECS task metadata", addr: "169.254.170.2", want: catMetadata},
		{name: "AWS IMDS over IPv6", addr: "fd00:ec2::254", want: catMetadata},
		{name: "Alibaba Cloud metadata", addr: "100.100.100.200", want: catMetadata},
		{name: "Oracle Cloud metadata", addr: "192.0.0.192", want: catMetadata},

		// Every IPv6 form that can name an internal IPv4 target.
		{name: "IPv4-mapped loopback", addr: "::ffff:127.0.0.1", want: catLoopback},
		{name: "IPv4-mapped private", addr: "::ffff:10.0.0.1", want: catPrivate},
		{name: "IPv4-mapped metadata", addr: "::ffff:169.254.169.254", want: catMetadata},
		{name: "IPv4-compatible loopback", addr: "::7f00:1", want: catLoopback},
		{name: "NAT64 loopback", addr: "64:ff9b::7f00:1", want: catLoopback},
		{name: "NAT64 metadata", addr: "64:ff9b::a9fe:a9fe", want: catMetadata},
		{name: "6to4 loopback", addr: "2002:7f00:1::", want: catLoopback},
		{name: "6to4 public", addr: "2002:5db8:d822::", want: catPublic},

		{name: "IPv4-translated loopback", addr: "::ffff:0:7f00:1", want: catLoopback},
		{name: "IPv4-translated metadata", addr: "::ffff:0:a9fe:a9fe", want: catMetadata},

		// Ranges whose embedded IPv4 address cannot be located reliably must not
		// read as public, because every address in them reaches some IPv4 target.
		{name: "local-use NAT64", addr: "64:ff9b:1::7f00:1", want: catTranslation},
		{name: "Teredo", addr: "2001:0:4136:e378:8000:63bf:3fff:fdd2", want: catTranslation},

		// Ranges that no netip predicate reports as special.
		{name: "reserved 240/4", addr: "240.0.0.1", want: catReserved},
		{name: "benchmarking range", addr: "198.18.0.1", want: catReserved},
		{name: "deprecated site-local", addr: "fec0::1", want: catReserved},
		{name: "ORCHIDv2", addr: "2001:20::1", want: catReserved},

		// A zone must not hide an address from the prefix-based checks, which
		// report no match for any zoned address.
		{name: "zoned link-local", addr: "fe80::1%eth0", want: catLinkLocal},
		{name: "zoned unique local", addr: "fd00::1%eth0", want: catUniqueLocal},
		{name: "zoned metadata", addr: "fd00:ec2::254%eth0", want: catMetadata},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classify(netip.MustParseAddr(test.addr))
			require.Equal(t, string(test.want), string(got))
		})
	}
}

func Test_Policy_CheckAddr(t *testing.T) {
	tests := []struct {
		name  string
		opts  []Option
		addr  string
		check func(t *testing.T, err error)
	}{
		{
			name: "default allows public",
			addr: "93.184.216.34:443",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "default denies loopback",
			addr: "127.0.0.1:8080",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "loopback")
			},
		},
		{
			name: "default denies private",
			addr: "10.1.2.3:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "private")
			},
		},
		{
			name: "default denies cloud metadata",
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "cloud metadata")
			},
		},
		{
			name: "default denies IPv4-mapped loopback",
			addr: "[::ffff:127.0.0.1]:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "loopback")
			},
		},
		{
			name: "default denies NAT64 metadata",
			addr: "[64:ff9b::a9fe:a9fe]:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "cloud metadata")
			},
		},
		{
			name: "loopback opt-in allows loopback",
			opts: []Option{WithAllowLoopback()},
			addr: "127.0.0.1:8080",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "loopback opt-in still denies private",
			opts: []Option{WithAllowLoopback()},
			addr: "192.168.0.5:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "private")
			},
		},
		{
			name: "loopback opt-in still denies metadata",
			opts: []Option{WithAllowLoopback()},
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "cloud metadata")
			},
		},
		{
			name: "private opt-in allows private and unique local",
			opts: []Option{WithAllowPrivateNetworks()},
			addr: "[fc00::1]:80",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "link-local opt-in does not allow metadata",
			opts: []Option{WithAllowLinkLocal()},
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "cloud metadata")
			},
		},
		{
			name: "metadata opt-in allows metadata",
			opts: []Option{WithAllowCloudMetadata()},
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "allowed network exempts a private range",
			opts: []Option{WithAllowNetworks(netip.MustParsePrefix("10.0.0.0/8"))},
			addr: "10.9.8.7:443",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "allowed network excludes everything else",
			opts: []Option{WithAllowNetworks(netip.MustParsePrefix("10.0.0.0/8"))},
			addr: "93.184.216.34:443",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "outside every allowed network")
			},
		},
		{
			name: "denied network beats allowed network",
			opts: []Option{
				WithAllowNetworks(netip.MustParsePrefix("10.0.0.0/8")),
				WithDenyNetworks(netip.MustParsePrefix("10.1.0.0/16")),
			},
			addr: "10.1.2.3:443",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "denied network 10.1.0.0/16")
			},
		},
		{
			name: "denied network beats a category opt-in",
			opts: []Option{
				WithAllowLoopback(),
				WithDenyNetworks(netip.MustParsePrefix("127.0.0.0/8")),
			},
			addr: "127.0.0.1:8080",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "denied network 127.0.0.0/8")
			},
		},
		{
			// A prefix written in IPv4-mapped form would match nothing at all if it
			// were compared as given, so a deny list would silently do nothing.
			name: "an IPv4-mapped prefix denies the IPv4 range it names",
			opts: []Option{
				WithAllowPrivateNetworks(),
				WithDenyNetworks(netip.MustParsePrefix("::ffff:10.0.0.0/104")),
			},
			addr: "10.0.0.1:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "denied network 10.0.0.0/8")
			},
		},
		{
			// The prefix lists look through the IPv4-in-IPv6 forms too, or a deny
			// list would be weaker than the category checks.
			name: "a denied network also denies its NAT64 form",
			opts: []Option{
				WithAllowCloudMetadata(),
				WithAllowLinkLocal(),
				WithDenyNetworks(netip.MustParsePrefix("169.254.0.0/16")),
			},
			addr: "[64:ff9b::a9fe:a9fe]:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "denied network 169.254.0.0/16")
			},
		},
		{
			// A broad network allowance must not quietly grant what the link-local
			// opt-in deliberately withholds.
			name: "an allowed network does not grant cloud metadata",
			opts: []Option{WithAllowNetworks(netip.MustParsePrefix("169.254.0.0/16"))},
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "even inside an allowed network")
			},
		},
		{
			name: "an allowed network grants metadata when metadata is opted into",
			opts: []Option{
				WithAllowCloudMetadata(),
				WithAllowNetworks(netip.MustParsePrefix("169.254.0.0/16")),
			},
			addr: "169.254.169.254:80",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "port 0 is not a destination",
			opts: []Option{WithAllowLoopback()},
			addr: "127.0.0.1:0",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonPort, "port 0")
			},
		},
		{
			name: "a zone does not evade a denied network",
			opts: []Option{WithDenyNetworks(netip.MustParsePrefix("fd00::/8"))},
			addr: "[fd00::1%eth0]:80",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonAddress, "denied network fd00::/8")
			},
		},
		{
			name: "denied port",
			opts: []Option{WithAllowLoopback(), WithDenyPorts(22)},
			addr: "127.0.0.1:22",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonPort, "denied")
			},
		},
		{
			name: "port outside allowlist",
			opts: []Option{WithAllowLoopback(), WithAllowPorts(443)},
			addr: "127.0.0.1:8080",
			check: func(t *testing.T, err error) {
				requireDenied(t, err, ReasonPort, "not allowed")
			},
		},
		{
			name: "port inside allowlist",
			opts: []Option{WithAllowLoopback(), WithAllowPorts(443, 8443)},
			addr: "127.0.0.1:8443",
			check: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := New(test.opts...)
			require.NoError(t, err)

			test.check(t, policy.CheckAddr(netip.MustParseAddrPort(test.addr)))
		})
	}
}

func Test_Policy_CheckAddr_invalid(t *testing.T) {
	policy, err := New()
	require.NoError(t, err)

	requireDenied(t, policy.CheckAddr(netip.AddrPort{}), ReasonRequest, "not a valid IP address")
}
