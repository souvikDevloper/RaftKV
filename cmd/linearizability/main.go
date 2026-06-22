package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

type historyRecord struct {
	ID       int    `json:"id"`
	ClientID int    `json:"client_id"`
	Op       string `json:"op"`
	Value    string `json:"value"`
	Output   string `json:"output"`
	Call     int64  `json:"call_ns"`
	Return   int64  `json:"return_ns"`
	OK       bool   `json:"ok"`
}
type registerInput struct{ Op, Value string }
type timedEvent struct {
	Timestamp int64
	Event     porcupine.Event
}

func main() {
	history := flag.String("history", "run/history.jsonl", "JSONL operation history")
	timeout := flag.Duration("timeout", 30*time.Second, "checker timeout")
	flag.Parse()
	file, err := os.Open(*history)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	var records []historyRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record historyRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			panic(err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	ordered := make([]timedEvent, 0, len(records)*2)
	for _, record := range records {
		ordered = append(ordered, timedEvent{record.Call, porcupine.Event{ClientId: record.ClientID, Kind: porcupine.CallEvent, Value: registerInput{record.Op, record.Value}, Id: record.ID}})
		if record.OK {
			output := any(true)
			if record.Op == "get" {
				output = record.Output
			}
			ordered = append(ordered, timedEvent{record.Return, porcupine.Event{ClientId: record.ClientID, Kind: porcupine.ReturnEvent, Value: output, Id: record.ID}})
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Timestamp < ordered[j].Timestamp })
	events := make([]porcupine.Event, len(ordered))
	for index := range ordered {
		events[index] = ordered[index].Event
	}
	model := porcupine.Model{Init: func() any { return "" }, Step: func(state, input, output any) (bool, any) {
		operation := input.(registerInput)
		if operation.Op == "put" {
			return output == true, operation.Value
		}
		return output == state, state
	}}
	result := porcupine.CheckEventsTimeout(model, events, *timeout)
	if result != porcupine.Ok {
		fmt.Printf("FAIL: history is %s (%d calls, including indeterminate failures)\n", result, len(records))
		os.Exit(1)
	}
	fmt.Printf("PASS: Porcupine proved history linearizable (%d calls, including indeterminate failures)\n", len(records))
}
