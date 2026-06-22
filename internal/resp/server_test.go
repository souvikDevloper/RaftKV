package resp

import (
	"bufio"
	"strings"
	"testing"
)

func TestReadRESPCommand(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("*3\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nf\r\n"))
	command, err := readCommand(reader)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(command, ",") != "HSET,k,f" {
		t.Fatalf("unexpected command: %#v", command)
	}
}
