package main

import (
	mr "mapreduce/lib"
	"strconv"
	"strings"
)

var searchWord = "MapReduce"

func Map(filename string, contents string) []mr.KeyValue {
	lines := strings.Split(contents, "\n")
	kva := make([]mr.KeyValue, 0)
	for _, line := range lines {
		if strings.Contains(line, searchWord) {
			kva = append(kva, mr.KeyValue{Key: filename, Value: "1"})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
