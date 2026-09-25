package main

import (
	"maps"
	"net/url"
	"slices"
)

// urlQueryEscape is the one escaping this plugin does. Repositories, tags and
// digests are validated against their own grammars before they reach a path,
// so the only value that can hold a character with meaning in a URL is an
// artifact type, which is a media type from a workflow's own input.
func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

// sortedKeys returns a map's keys in order, so that what a task writes into
// durable history does not depend on map iteration.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
