package flowstatev1

// TypeText is the type an input declares, spelled the way a Flowfile author
// writes it: `list(string)`, `map(string, int)`, `string`.
//
// It is what a diagnostic, a hover or a completion detail should show, because
// it is the text the author can paste back into `type:`. The legacy enum alone
// would call a `list(string)` input just `list`, which says less than the file
// does. A declaration with no type answers the legacy name for it, so a caller
// never has to handle an empty string.
func (x *InputDeclaration) TypeText() string {
	return declaredTypeText(x.DeclaredType(), x.GetType())
}

// TypeText is the type an output declares, by the rule
// [InputDeclaration.TypeText] states.
func (x *OutputDeclaration) TypeText() string {
	return declaredTypeText(x.DeclaredType(), x.GetType())
}

func declaredTypeText(structural *Type, legacy InputDeclaration_Type) string {
	if structural == nil {
		return DeclaredTypeName(legacy)
	}

	return TypeString(structural)
}
