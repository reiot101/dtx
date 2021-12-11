package internal

import "reflect"

// ParseType parsing struct or point to reflect type
func ParseType(v interface{}) reflect.Type {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
