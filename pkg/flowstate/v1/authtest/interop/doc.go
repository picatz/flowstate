// Package interop provides a hermetic, deliberately misbehaving federation
// laboratory for black-box interoperability tests.
//
// Environment owns loopback HTTP and Unix-domain-socket listeners. Tests alter
// behaviour with scripts rather than provider-shaped test doubles, then run the
// same Suite against Flowstate or an explicitly enabled external Adapter.
// ProviderExpectations are reported separately and never change whether a
// standards case passed.
package interop
