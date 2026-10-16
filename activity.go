package durable

import (
	"context"
	"fmt"
	"reflect"
)

// ActivityDefinition ties one durable name to its Go input and output types.
// Keep names and types compatible while executions using them are in progress.
type ActivityDefinition[Input, Output any] struct {
	name string
}

func DefineActivity[Input, Output any](name string) ActivityDefinition[Input, Output] {
	return ActivityDefinition[Input, Output]{name: name}
}

func (a ActivityDefinition[Input, Output]) contract() string {
	return typeIdentity(reflect.TypeFor[Input]()) + " -> " + typeIdentity(reflect.TypeFor[Output]())
}

func typeIdentity(t reflect.Type) string {
	if t.Name() != "" {
		return t.PkgPath() + "/" + t.String()
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return fmt.Sprintf("%s[%d]%s", t.Kind(), arrayLength(t), typeIdentity(t.Elem()))
	case reflect.Map:
		return "map[" + typeIdentity(t.Key()) + "]" + typeIdentity(t.Elem())
	case reflect.Struct:
		identity := "struct{"
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			identity += field.PkgPath + "/" + field.Name + ":" + typeIdentity(field.Type) + ":" + string(field.Tag) + ";"
		}
		return identity + "}"
	default:
		return t.String()
	}
}

func arrayLength(t reflect.Type) int {
	if t.Kind() == reflect.Array {
		return t.Len()
	}
	return 0
}

// Step binds input to a stable key within a workflow. Keys are independent of
// activity names, so one activity definition can appear more than once.
func (a ActivityDefinition[Input, Output]) Step(key string, input Input) (ActivitySpec, error) {
	if key == "" {
		return ActivitySpec{}, fmt.Errorf("step key is required")
	}
	spec, err := NewActivity(a.name, input)
	if err != nil {
		return ActivitySpec{}, err
	}
	spec.Key, spec.contract = key, a.contract()
	return spec, nil
}

func (a ActivityDefinition[Input, Output]) Handle(handler func(context.Context, Input) (Output, error)) HandlerRegistration {
	registration := Handle(a.name, handler)
	registration.contract = a.contract()
	return registration
}
