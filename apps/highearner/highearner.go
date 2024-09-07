package main

import (
	mr "mapreduce/lib"
	"strconv"
	"strings"
)

func Map(filename string, contents string) []mr.KeyValue {
	lines := strings.Split(contents, "\n")
	kva := make([]mr.KeyValue, 0)

	for _, line := range lines {
		if line == "" {
			continue
		}

		fields := strings.Split(line, ",")

		salaryStr := strings.TrimSpace(fields[len(fields)-1])
		salary, err := strconv.Atoi(salaryStr)
		if err != nil {
			continue
		}

		if salary >= 100000 {
			kva = append(kva, mr.KeyValue{Key: filename, Value: "1"})
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
