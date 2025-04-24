package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const RETRY = 0
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	retry := 0
	taskID := -1
	args := ExampleArgs{
		TaskID: taskID,
		TaskType: 0,
	}
	// reply := ExampleReply{}
	for {
		reply := ExampleReply{}
		err := CallExample(args, &reply)
		if err != nil {
			if retry <= RETRY {
				retry += 1
				time.Sleep(10 * time.Second) 
				continue
			} else {
				os.Exit(0)
			}
		} 
		// fmt.Printf("YY_DEBUG reply from coordinator %+v", reply)
		retry = 0
		if reply.PleaseExit {
			os.Exit(0)
		}
		if reply.TaskType == 1 {
			intermediate := map[int][]string{}
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			for _, kv := range kva {
				idx := ihash(kv.Key)%reply.NReduce
				intermediate[idx] = append(intermediate[idx], kv.Key)
			}
			writeToMapFile(intermediate, reply.TaskID)
			args.TaskID = reply.TaskID
			args.TaskType = reply.TaskType
		} else if reply.TaskType == 2 {
			files, err := os.ReadDir(".")
			if err != nil {
				fmt.Println("Error reading directory:", err)
				return
			}
			final := []KeyValue{}
			// Iterate through all the files in the directory
			for _, file := range files {
				// Check if it's a file and ends with "taks"
				parts := strings.Split(file.Name(), "-")
				if file.Type().IsRegular() && parts[len(parts)-1] == strconv.Itoa(reply.TaskID) {
					
					// fmt.Println("Opening reduce file:", file.Name())
		
					// Open the file for appending
					file, err := os.Open(file.Name())
					if err != nil {
						fmt.Printf("Error opening file %s: %v\n", file.Name(), err)
						continue
					}
					defer file.Close()
				
					kva := []KeyValue{}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							fmt.Printf("YY_DEBUG error decoding map file name %+v, err %+v", file.Name(), err)
							break
						}
						kva = append(kva, kv)
					}
					
					file.Close()
					sort.Sort(ByKey(kva))
					i := 0
					for i < len(kva) {
						j := i + 1
						for j < len(kva) && kva[j].Key == kva[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, kva[k].Value)
						}
						output := reducef(kva[i].Key, values)
						final = append(final, KeyValue{kva[i].Key, output})
						i = j
					}
					// fmt.Printf("YY_DEBUG reduce decode done %+v", reply)
				}
			}
			writeToReduceFile(final, reply.TaskID)
			args.TaskID = reply.TaskID
			args.TaskType = reply.TaskType
		} else {
			// fmt.Printf("YY_DEBUG worker receives reply %+v", reply)
			time.Sleep(1 * time.Second)
		}
	}
}

func writeToMapFile(data map[int][]string, taskID int) {
	// Create a temporary file in the current directory
	for reduceIdx, contents := range data {
		tmpFile, err := os.CreateTemp(".", "temp-file-*")
		if err != nil {
			fmt.Println("Error creating temporary map file:", err)
			return
		}
		defer os.Remove(tmpFile.Name()) // Clean up the temporary file when done
	
		// fmt.Println("Created temporary map file:", tmpFile.Name())
		enc := json.NewEncoder(tmpFile)
		for i :=range contents {
			kv := KeyValue{
				Key: contents[i],
				Value: "1",
			}
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Error writing to temporary map file:", err)
				return
			}
		}
		// Close the file after creation
		err = tmpFile.Close()
		if err != nil {
			fmt.Println("Error closing temporary map file:", err)
			return
		}
	
		// Define the new file name
		newFileName := "mr-" + strconv.Itoa(taskID)+ "-" + strconv.Itoa(reduceIdx)
		tmpFileName := tmpFile.Name()
		// Rename the temporary file
		err = os.Rename(tmpFileName, newFileName)
		if err != nil {
			fmt.Println("Error renaming map file:", err)
			return
		}
	
		// fmt.Println("Renamed map file to:", newFileName)
	
		// Verify the renamed file exists
		_, err = os.Stat(newFileName)
		if err != nil {
			fmt.Println("Error verifying renamed map file:", err)
			return
		}
		// fmt.Println("Renamed map file exists at:", filepath.Join(".", newFileName))
	}
}

func writeToReduceFile(kva []KeyValue, taskID int) {
	tmpFile, err := os.CreateTemp(".", "temp-file-*")
	if err != nil {
		fmt.Println("Error creating temporary file:", err)
		return
	}
	defer os.Remove(tmpFile.Name()) // Clean up the temporary file when done

	for i := range kva {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, kva[i].Value)
	}
	newFileName := "mr-out-" + strconv.Itoa(taskID)
	tmpFileName := tmpFile.Name()
	// Rename the temporary file
	err = os.Rename(tmpFileName, newFileName)
	if err != nil {
		fmt.Println("Error renaming temporary file:", err)
		return
	}

	// fmt.Println("Renamed temporary reduce file to:", newFileName)
	// Close the file after creation
	err = tmpFile.Close()
	if err != nil {
		fmt.Println("Error closing temporary reduce file:", err)
		return
	}
	// Verify the renamed file exists
	_, err = os.Stat(newFileName)
	if err != nil {
		fmt.Println("Error verifying renamed reduce file:", err)
		return
	}
	// fmt.Println("Renamed reduce file exists at:", filepath.Join(".", newFileName))
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(args ExampleArgs, reply *ExampleReply) error {

	// // declare an argument structure.
	// args := ExampleArgs{}
	// // declare a reply structure.
	// reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, reply)
	if ok {
		// fmt.Printf("reply %#v\n", reply)
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return fmt.Errorf("call failed reply: %v", reply)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
