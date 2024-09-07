package main

import (
	mr "mapreduce/lib"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mr.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, ff)
	kva := make([]mr.KeyValue, 0)
	for _, word := range words {
		kva = append(kva, mr.KeyValue{Key: word, Value: "1"})
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
