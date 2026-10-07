package core

// ThreadActive reports whether a threads.json entry is still watched
// (IS-4): false when status is the string "done" or "closed", or when
// closed is present with a non-empty string value; true otherwise,
// including a nil map, a non-string status, and a null, empty or
// non-string closed value.
func ThreadActive(m *OMap) bool {
	if m == nil {
		return true
	}
	if v, ok := m.Get("status"); ok {
		if s, isStr := v.(string); isStr && (s == "done" || s == "closed") {
			return false
		}
	}
	if v, ok := m.Get("closed"); ok && v != nil {
		if s, isStr := v.(string); isStr && s != "" {
			return false
		}
	}
	return true
}
