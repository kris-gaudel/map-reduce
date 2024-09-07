package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapFunc func(string, string) []KeyValue, reduceFunc func(string, []string) string) {
	for {
		reply, err := CallGetTask()
		fmt.Println("Reply name: ", reply.Name)
		if err != nil {
			log.Fatal(err)
		}

		if reply.Type == mapType {
			executeMapTask(reply.Name, reply.Number, reply.NReduce, mapFunc)
			CallMarkTaskAsDone(mapType, reply.Name)
		} else {
			executeReduceTask(reply.Number, reduceFunc)
			CallMarkTaskAsDone(reduceType, reply.Name)
		}
	}
}

func executeMapTask(fileName string, mapNumber, numReduce int, mapFunc func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	fmt.Println("fileName ", fileName)
	if err != nil {
		log.Fatalf("Cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", fileName)
	}
	file.Close()
	kva := mapFunc(fileName, string(content))
	mp := map[int]*os.File{}
	for _, kv := range kva {
		reduceNumber := ihash(kv.Key) % numReduce
		f, ok := mp[reduceNumber]
		if !ok {
			f, err = ioutil.TempFile("", "tmp")
			mp[reduceNumber] = f
			if err != nil {
				log.Fatal(err)
			}
		}
		kvj, _ := json.Marshal(kv)
		fmt.Fprintf(f, "%s\n", kvj)
	}

	for reduceNum, f := range mp {
		os.Rename(f.Name(), fmt.Sprintf("mr-%d-%d", mapNumber, reduceNum))
		f.Close()
	}
}

func WalkDir(root string, rNumber int) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		matched, merr := regexp.Match(fmt.Sprintf((`mr-\d-%d`), rNumber), []byte(path))
		if merr != nil {
			return merr
		}
		if matched {
			files = append(files, path)
			return nil
		}

		return nil
	})
	return files, err
}

func executeReduceTask(rNumber int, reducef func(string, []string) string) {
	filenames, _ := WalkDir("./", rNumber)
	data := make([]KeyValue, 0)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open %v. Error: %s", filename, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Cannot read %v. Error: %s", filename, err)
		}
		file.Close()

		kvstrings := strings.Split(string(content), "\n")
		kv := KeyValue{}
		for _, kvstring := range kvstrings[:len(kvstrings)-1] {
			err := json.Unmarshal([]byte(kvstring), &kv)
			if err != nil {
				log.Fatalf("Cannot unmarshal %v. Error: %s", filename, err)
			}
			data = append(data, kv)
		}

	}
	sort.Sort(ByKey(data))

	oname := fmt.Sprintf("mr-out-%d", rNumber)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

		i = j
	}

	ofile.Close()
}

func CallGetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Master.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Name '%v', reply.Type '%v'\n", reply.Name, reply.Type)
		return &reply, nil
	} else {
		return nil, errors.New("call failed")
	}
}

func CallMarkTaskAsDone(typ TaskType, name string) error {
	args := UpdateTaskStatusArgs{
		Name: name,
		Type: typ,
	}
	reply := UpdateTaskStatusReply{}

	ok := call("Master.UpdateTaskStatus", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("call failed")
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSocket()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Fatalf(err.Error())
	return false
}
