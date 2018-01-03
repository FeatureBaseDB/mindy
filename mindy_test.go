package mindy

import (
	"fmt"
	"testing"

	"github.com/pilosa/go-pilosa"

	ptest "github.com/pilosa/pilosa/test"
)

func TestMindy(t *testing.T) {
	server := ptest.MustNewRunningServer(t)
	populate(t, server.Server.Addr().String())

	m := NewMain()
	m.Pilosa = []string{server.Server.Addr().String()}
	m.Bind = "localhost:33333"
	err := m.listen()
	if err != nil {
		t.Fatalf("m.listen: %v", err)
	}

	go m.serve()

	client := Client{
		Addr: m.Bind,
	}

	tests := []struct {
		req      *Request
		expected map[string]struct{}
	}{
		{
			req: &Request{
				Indexes:     []string{"p1", "two", "p3"},
				Includes:    []Row{{ID: 0, Frame: "f1"}, {ID: 0, Frame: "f2"}},
				Excludes:    []Row{},
				Conjunction: "and",
			},
			expected: bits(
				iss{"p1", 1, 2},
				iss{"p3", 3, 6},
			),
		},
	}

	for i, test := range tests {
		scanner, err := client.Post(test.req)
		if err != nil {
			t.Fatalf("making request %d: %v", i, err)
		}
		for scanner.Scan() {
			line := scanner.Text()
			if _, ok := test.expected[line]; !ok {
				t.Fatalf("test %d: '%s' in response, but not expected", i, line)
			}
			delete(test.expected, line)
		}
		if len(test.expected) > 0 {
			t.Fatalf("test %d: leftover items in expected: %v", i, test.expected)
		}
	}
}

func populate(t *testing.T, host string) {
	client, err := pilosa.NewClientFromAddresses([]string{host}, nil)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}

	indexes := []string{"p1", "two", "p3", "p4"}
	for i, idx := range indexes {
		i := uint64(i)
		sch, err := client.Schema()
		if err != nil {
			t.Fatalf("getting schema: %v", err)
		}

		index, err := sch.Index(idx, nil)
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}

		f1, err := index.Frame("f1", nil)
		f2, err := index.Frame("f2", nil)

		err = client.SyncSchema(sch)
		if err != nil {
			t.Fatalf("syncing schema: %v", err)
		}

		bq := index.BatchQuery()
		for row := uint64(0); row < 5; row++ {
			for bit := uint64(0); bit < 100; bit += 1 + i + row {
				bq.Add(f1.SetBit(row, bit))
				if bit%2 == 1 {
					bq.Add(f2.SetBit(row, bit))
				}
			}
		}
		_, err = client.Query(bq, nil)
		if err != nil {
			t.Fatalf("querying: %v", err)
		}

	}

}

// iss is index,start,step
type iss struct {
	index string
	start uint64
	step  uint64
}

// bits constructs a map of index,column pairs based on the passed in specs.
func bits(specs ...iss) map[string]struct{} {
	idxs := make(map[string]struct{})
	for _, spec := range specs {
		for i := spec.start; i < 100; i += spec.step {
			idxs[fmt.Sprintf("%s,%d", spec.index, i)] = struct{}{}
		}
	}
	return idxs
}
