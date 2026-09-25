package docsgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// PolicyReference is the documentation-bearing shape of one operator policy.
// It is extracted from the Go struct the strict YAML decoder reads, rather than
// maintained beside that struct as another field list.
type PolicyReference struct {
	Description string
	Fields      []PolicyField
}

// PolicyField is one YAML field extracted from a policy struct.
type PolicyField struct {
	Name        string
	Type        string
	Description string
}

var goDocLink = regexp.MustCompile(`\[([A-Za-z0-9_.]+)\]`)

// ParsePolicyReference extracts structName's YAML field names, types, and Go
// documentation from source. Unsupported or undocumented fields are errors:
// adding a policy field must either make the reference truthful or fail the
// generation that pins it.
func ParsePolicyReference(source []byte, structName string) (PolicyReference, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", source, parser.ParseComments)
	if err != nil {
		return PolicyReference{}, fmt.Errorf("parsing policy source: %w", err)
	}

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != structName {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return PolicyReference{}, fmt.Errorf("%s is not a struct", structName)
			}

			doc := typeSpec.Doc
			if doc == nil {
				doc = general.Doc
			}
			if doc == nil || strings.TrimSpace(doc.Text()) == "" {
				return PolicyReference{}, fmt.Errorf("%s has no documentation", structName)
			}

			reference := PolicyReference{Description: policyProse(doc.Text())}
			for _, field := range structure.Fields.List {
				if len(field.Names) != 1 || field.Tag == nil {
					return PolicyReference{}, fmt.Errorf("%s has an embedded, grouped, or untagged field", structName)
				}

				tag, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					return PolicyReference{}, fmt.Errorf("%s.%s has an invalid struct tag: %w", structName, field.Names[0].Name, err)
				}
				yamlTag := reflect.StructTag(tag).Get("yaml")
				parts := strings.Split(yamlTag, ",")
				if yamlTag == "" || parts[0] == "" || parts[0] == "-" {
					return PolicyReference{}, fmt.Errorf("%s.%s has no documentable YAML name", structName, field.Names[0].Name)
				}

				if field.Doc == nil || strings.TrimSpace(field.Doc.Text()) == "" {
					return PolicyReference{}, fmt.Errorf("%s.%s has no field documentation", structName, field.Names[0].Name)
				}
				fieldType, err := policyYAMLType(field.Type)
				if err != nil {
					return PolicyReference{}, fmt.Errorf("%s.%s: %w", structName, field.Names[0].Name, err)
				}

				reference.Fields = append(reference.Fields, PolicyField{
					Name:        parts[0],
					Type:        fieldType,
					Description: policyProse(field.Doc.Text()),
				})
			}
			return reference, nil
		}
	}

	return PolicyReference{}, fmt.Errorf("policy struct %s was not found", structName)
}

func policyYAMLType(expression ast.Expr) (string, error) {
	switch typed := expression.(type) {
	case *ast.ArrayType:
		element, err := policyYAMLType(typed.Elt)
		if err != nil {
			return "", err
		}
		return "sequence of " + element, nil
	case *ast.StarExpr:
		return policyYAMLType(typed.X)
	case *ast.Ident:
		switch typed.Name {
		case "string":
			return "string", nil
		case "uint64", "int", "int64":
			return "integer", nil
		case "bool":
			return "boolean", nil
		default:
			return "", fmt.Errorf("unsupported policy field type %s", typed.Name)
		}
	case *ast.SelectorExpr:
		if qualifier, ok := typed.X.(*ast.Ident); ok && qualifier.Name == "time" && typed.Sel.Name == "Duration" {
			return "duration", nil
		}
		return "", fmt.Errorf("unsupported qualified policy field type")
	default:
		return "", fmt.Errorf("unsupported policy field type")
	}
}

func policyProse(text string) string {
	text = goDocLink.ReplaceAllString(text, "$1")
	return strings.Join(strings.Fields(text), " ")
}

func (g *Generator) renderTaskPolicyReference() string {
	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# Task-shape policy reference\n\n")
	b.WriteString(g.src.TaskPolicy.Description + "\n\n")
	b.WriteString("The field inventory below is extracted from `TaskPolicyConfig`, the Go struct\n")
	b.WriteString("the YAML decoder reads. It documents configuration shape; the compiled policy\n")
	b.WriteString("remains the authority for whether a task dispatch is allowed.\n\n")
	b.WriteString("| YAML field | Value | Meaning |\n|---|---|---|\n")
	for _, field := range g.src.TaskPolicy.Fields {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n",
			cell(field.Name), field.Type, cell(field.Description))
	}

	return b.String()
}
