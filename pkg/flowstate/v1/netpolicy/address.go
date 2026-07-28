package netpolicy

import (
	"net/netip"
	"slices"
)

// Networks that classification needs to recognise by prefix rather than by one of
// the netip.Addr predicates.
var (
	// zeroNetwork is "this network", which is not a valid destination.
	zeroNetwork = netip.MustParsePrefix("0.0.0.0/8")

	// carrierGradeNetwork is RFC 6598 shared address space. It is not private
	// under RFC 1918 but is not reachable across the public internet either, and
	// Alibaba Cloud puts its metadata service inside it.
	carrierGradeNetwork = netip.MustParsePrefix("100.64.0.0/10")

	// uniqueLocalNetwork is RFC 4193 IPv6 unique local address space.
	uniqueLocalNetwork = netip.MustParsePrefix("fc00::/7")

	// ipv4Broadcast is the limited broadcast address.
	ipv4Broadcast = netip.MustParseAddr("255.255.255.255")

	// reservedNetworks are ranges that are not usable destinations for a workflow
	// but that none of the netip predicates report as special, so they would
	// otherwise be classified as public.
	reservedNetworks = []netip.Prefix{
		netip.MustParsePrefix("192.0.0.0/24"),  // RFC 6890 IETF protocol assignments
		netip.MustParsePrefix("198.18.0.0/15"), // RFC 2544 benchmarking
		netip.MustParsePrefix("240.0.0.0/4"),   // RFC 1112 reserved
		netip.MustParsePrefix("fec0::/10"),     // RFC 3879 deprecated site-local
		netip.MustParsePrefix("2001:20::/28"),  // RFC 7343 ORCHIDv2
	}

	// translationNetworks carry an IPv4 destination whose position depends on the
	// translator's configuration, so the embedded address cannot be extracted
	// reliably. Every address in them reaches some IPv4 target, which the IPv4
	// policy would have to decide about, so none of them can be treated as public.
	translationNetworks = []netip.Prefix{
		netip.MustParsePrefix("64:ff9b:1::/48"), // RFC 8215 local-use NAT64
		netip.MustParsePrefix("2001::/32"),      // RFC 4380 Teredo
	}
)

// Prefixes that embed an IPv4 address at a fixed position inside an IPv6 address.
// Each is a way to name an internal IPv4 target with an address that none of the
// IPv6 predicates consider special, so the embedded address is classified as well.
var (
	// ipv4CompatibleNetwork is the deprecated ::/96 form, written as ::127.0.0.1.
	ipv4CompatibleNetwork = netip.MustParsePrefix("::/96")

	// ipv4TranslatedNetwork is the RFC 2765 SIIT form, ::ffff:0:127.0.0.1.
	ipv4TranslatedNetwork = netip.MustParsePrefix("::ffff:0:0:0/96")

	// nat64Network is the RFC 6052 well-known prefix, written as 64:ff9b::7f00:1.
	nat64Network = netip.MustParsePrefix("64:ff9b::/96")

	// sixToFourNetwork is RFC 3056 6to4 space, which carries the IPv4 address in
	// the 32 bits after the prefix.
	sixToFourNetwork = netip.MustParsePrefix("2002::/16")
)

// metadataAddrs are the well-known cloud instance metadata endpoints. They hand
// out credentials and instance identity to any local caller, which makes them the
// highest-value target of a server-side request forgery.
var metadataAddrs = map[netip.Addr]struct{}{
	netip.MustParseAddr("169.254.169.254"): {}, // AWS, Azure, GCP, OpenStack, DigitalOcean
	netip.MustParseAddr("169.254.169.253"): {}, // AWS VPC DNS
	netip.MustParseAddr("169.254.169.123"): {}, // AWS VPC NTP
	netip.MustParseAddr("169.254.170.2"):   {}, // AWS ECS task metadata and credentials
	netip.MustParseAddr("169.254.170.23"):  {}, // AWS ECS task metadata (IPv6 host, v4 form)
	netip.MustParseAddr("100.100.100.200"): {}, // Alibaba Cloud
	netip.MustParseAddr("192.0.0.192"):     {}, // Oracle Cloud, legacy endpoint
	netip.MustParseAddr("fd00:ec2::254"):   {}, // AWS IMDS over IPv6
	netip.MustParseAddr("fd00:ec2::23"):    {}, // AWS ECS task metadata over IPv6
}

// normalize puts an address into the form the checks expect: IPv4-mapped
// addresses become IPv4, and any IPv6 zone is dropped. Dropping the zone matters
// because [netip.Prefix.Contains] reports false for every zoned address, so a
// zoned address would otherwise slip past the network lists.
func normalize(addr netip.Addr) netip.Addr {
	addr = addr.Unmap()
	if addr.Zone() != "" {
		addr = addr.WithZone("")
	}
	return addr
}

// classify returns the category of addr, resolving IPv4-mapped and IPv4-embedding
// IPv6 forms first so that ::ffff:127.0.0.1 and ::7f00:1 are treated as the
// loopback addresses they reach.
func classify(addr netip.Addr) category {
	addr = normalize(addr)

	cat := classifyDirect(addr)
	if cat != catPublic {
		return cat
	}

	// A public-looking IPv6 address may still name an internal IPv4 target.
	if embedded, ok := embeddedIPv4(addr); ok {
		if embeddedCat := classifyDirect(embedded); embeddedCat != catPublic {
			return embeddedCat
		}
	}

	return catPublic
}

// classifyDirect categorises an unmapped address without considering embedded
// IPv4 addresses.
func classifyDirect(addr netip.Addr) category {
	// Metadata addresses are checked first: most of them are also link-local or
	// carrier-grade NAT, and naming them specifically produces both a better
	// error message and an opt-in that does not drag in a whole range.
	if _, ok := metadataAddrs[addr]; ok {
		return catMetadata
	}

	switch {
	case addr.IsUnspecified(), zeroNetwork.Contains(addr):
		return catUnspecified
	case addr.IsLoopback():
		return catLoopback
	case addr == ipv4Broadcast:
		return catBroadcast
	case containsAny(translationNetworks, addr):
		return catTranslation
	case containsAny(reservedNetworks, addr):
		return catReserved
	case addr.IsMulticast(), addr.IsInterfaceLocalMulticast():
		// Checked before link-local: link-local multicast is both, and the
		// multicast category is the more useful thing to report.
		return catMulticast
	case addr.IsLinkLocalUnicast():
		return catLinkLocal
	case uniqueLocalNetwork.Contains(addr):
		return catUniqueLocal
	case carrierGradeNetwork.Contains(addr):
		return catCarrierGrade
	case addr.IsPrivate():
		return catPrivate
	default:
		return catPublic
	}
}

// embeddedIPv4 reports whether addr is an IPv6 address that carries an IPv4
// address at a known position, returning the embedded address.
func embeddedIPv4(addr netip.Addr) (netip.Addr, bool) {
	if !addr.Is6() {
		return netip.Addr{}, false
	}

	b := addr.As16()

	switch {
	case ipv4CompatibleNetwork.Contains(addr),
		ipv4TranslatedNetwork.Contains(addr),
		nat64Network.Contains(addr):
		return netip.AddrFrom4([4]byte(b[12:16])), true
	case sixToFourNetwork.Contains(addr):
		return netip.AddrFrom4([4]byte(b[2:6])), true
	default:
		return netip.Addr{}, false
	}
}

// containsAny reports whether any of the prefixes contains addr.
func containsAny(prefixes []netip.Prefix, addr netip.Addr) bool {
	return slices.ContainsFunc(prefixes, func(prefix netip.Prefix) bool {
		return prefix.Contains(addr)
	})
}

// CheckAddr reports whether p permits a connection to the given resolved address.
// It is the check the policy's dialer applies to every address it is about to
// connect to, exported so that callers can validate a configured endpoint ahead
// of time, such as when linting a workflow definition. It does not evaluate CEL
// rules, which need request attributes that a bare address does not carry.
//
// The returned error wraps [ErrDenied] and is a [*DenyError].
func (p *Policy) CheckAddr(addr netip.AddrPort) error {
	if !addr.IsValid() {
		return &DenyError{
			Reason: ReasonRequest,
			Target: addr.String(),
			Detail: "not a valid IP address and port",
		}
	}

	if addr.Port() == 0 {
		return &DenyError{
			Reason: ReasonPort,
			Target: addr.String(),
			Detail: "port 0 is not a valid destination",
		}
	}

	if err := p.checkPort(addr.Port(), addr.String()); err != nil {
		return err
	}

	ip := normalize(addr.Addr())
	target := ip.String()

	// A denied network is matched against the embedded IPv4 address as well as the
	// address itself, so a prefix cannot be sidestepped by naming its target
	// through NAT64 or one of the other IPv4-in-IPv6 forms. That expansion belongs
	// to the deny side only, and the asymmetry is the point: widening the set of
	// addresses a rule *catches* makes a denial harder to evade, and widening the
	// set a rule *permits* does the opposite.
	//
	// It was shared, and that let an allowance name one address and grant another:
	// with `WithAllowNetworks(10.0.0.0/8)` and nothing else, both `2002:a00:1::1`
	// and `64:ff9b::a00:1` were allowed. Neither is in 10.0.0.0/8 — they merely
	// carry it — and 2002::/16 is globally routable, so the destination reached is
	// a real host somewhere else. An allowlist exists to be exhaustive; one that
	// permits an address it never named is not one.

	if err := p.checkDeniedNetworks(addr); err != nil {
		return err
	}

	cat := classify(ip)

	if len(p.cfg.allowNetworks) > 0 {
		for _, allowed := range p.cfg.allowNetworks {
			// The address actually being dialled, and only that one.
			if allowed.Contains(ip) {
				// An allowed network exempts an address from the category denials,
				// but never from the metadata denial: handing a workflow the
				// credentials of the instance it runs on takes its own opt-in, so
				// that a broad network allowance cannot grant it by accident.
				if cat == catMetadata && !p.cfg.allowed[catMetadata] {
					return &DenyError{
						Reason: ReasonAddress,
						Target: target,
						Detail: "cloud metadata addresses are not allowed, even inside an allowed network",
					}
				}
				return nil
			}
		}
		return &DenyError{
			Reason: ReasonAddress,
			Target: target,
			Detail: "outside every allowed network",
		}
	}

	if !p.cfg.allowed[cat] {
		return &DenyError{
			Reason: ReasonAddress,
			Target: target,
			Detail: string(cat) + " addresses are not allowed",
		}
	}

	return nil
}

// checkDeniedNetworks reports whether an address falls in a denied network.
//
// It is separate so that the control-plane path can apply it too: a declared
// control plane is permitted on the operator's word, but an explicit denial is
// still a denial, which lets one address be carved out without withdrawing the
// capability.
func (p *Policy) checkDeniedNetworks(addr netip.AddrPort) error {
	if len(p.cfg.denyNetworks) == 0 {
		return nil
	}

	ip := normalize(addr.Addr())

	candidates := []netip.Addr{ip}
	if embedded, ok := embeddedIPv4(ip); ok {
		candidates = append(candidates, embedded)
	}

	for _, denied := range p.cfg.denyNetworks {
		for _, candidate := range candidates {
			if denied.Contains(candidate) {
				return &DenyError{
					Reason: ReasonAddress,
					Target: ip.String(),
					Detail: "within denied network " + denied.String(),
				}
			}
		}
	}

	return nil
}

// checkPort applies the port allow and deny lists. target describes what is being
// denied for the error message.
func (p *Policy) checkPort(port uint16, target string) error {
	if _, denied := p.cfg.denyPorts[port]; denied {
		return &DenyError{
			Reason: ReasonPort,
			Target: target,
			Detail: "port is denied",
		}
	}

	if len(p.cfg.allowPorts) > 0 {
		if _, allowed := p.cfg.allowPorts[port]; !allowed {
			return &DenyError{
				Reason: ReasonPort,
				Target: target,
				Detail: "port is not allowed",
			}
		}
	}

	return nil
}
