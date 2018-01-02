// mindy is a Multi INDex proxY for Pilosa. It should be used with a Pilosa
// instance which has multiple indexes which have distinct columns, but their
// rows all mean the same thing. Its inputs are:
// 1. the set of indexes to query.
// 2. a list of row,frame pairs to include.
// 3. a list of row,frame pairs to exclude.
// 4. the conjunction (AND or OR) which determines whether it will be an Intersect or Union respectively.
package mindy

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Main struct {
	Pilosa []string `help:"Comma separated list of pilosa hosts/ports."`
	Bind   string   `help:"Host/port to bind to."`
	s      *http.Server
	ln     net.Listener
}

func NewMain() *Main {
	return &Main{
		Pilosa: []string{"localhost:10101"},
		Bind:   ":10001",
	}
}
func (m *Main) Run() error {
	err := m.listen()
	if err != nil {
		errors.Wrap(err, "Main.listen")
	}
	err = m.serve()
	return errors.Wrap(err, "Main.serve")
}

func (m *Main) serve() error {
	return m.s.Serve(tcpKeepAliveListener{m.ln.(*net.TCPListener)})
}

func (m *Main) listen() error {
	client, err := pilosa.NewClientFromAddresses(m.Pilosa, nil)
	if err != nil {
		return errors.Wrap(err, "creating Pilosa client")
	}
	h := &Handler{
		client: client,
		sem:    make(semaphore, 2), // length of semaphore is number of concurrent goroutines querying pilosa.
	}

	sm := http.NewServeMux()
	sm.HandleFunc("/mindy", h.handleMindy)
	m.s = &http.Server{
		Addr:    m.Bind,
		Handler: sm,
	}
	if m.Bind == "" {
		m.Bind = ":http"
	}
	m.ln, err = net.Listen("tcp", m.Bind)
	return errors.Wrap(err, "starting listener")
}

type Row struct {
	ID    uint64 `json:"id"`
	Frame string `json:"frame"`
}

type Request struct {
	Indexes     []string `json:"indexes"`
	Includes    []Row    `json:"includes"`
	Excludes    []Row    `json:"excludes"`
	Conjunction string   `json:"conjunction"`
}

type semaphore chan struct{}

func (s semaphore) Acquire() {
	s <- struct{}{}
}

func (s semaphore) Release() {
	<-s
}

type Handler struct {
	client *pilosa.Client
	sem    semaphore
}

func (h *Handler) handleMindy(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "must POST to /mindy", http.StatusMethodNotAllowed)
		return
	}
	dec := json.NewDecoder(r.Body)
	req := &Request{}
	err := dec.Decode(req)
	if err != nil {
		http.Error(w, "decoding: "+err.Error(), http.StatusBadRequest)
		return
	}

	results, err := h.Query(req)
	if err != nil {
		http.Error(w, "querying pilosa: "+err.Error(), http.StatusInternalServerError)
		return
	}

	enc := json.NewEncoder(w)
	err = enc.Encode(results)
	if err != nil {
		http.Error(w, "encoding response"+err.Error(), http.StatusInternalServerError)
		return
	}

}

type Results struct {
	mu   sync.Mutex
	Bits map[string][]uint64 `json:"bits"`
}

func (r *Results) setIndex(idx string, bits []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Bits[idx] = bits
}

func (r *Results) Equals(r2 *Results) error {
	for prop, bits := range r.Bits {
		if bits2, ok := r2.Bits[prop]; !ok {
			return errors.Errorf("prop: '%v' in receiver, but not arg", prop)
		} else if len(bits2) != len(bits) {
			return errors.Errorf("different numbers of bits at prop: '%v", prop)
		} else {
			for i, _ := range bits {
				if bits[i] != bits2[i] {
					return errors.Errorf("bits not equal at prop '%v', index %d", prop, i)
				}
			}
		}
	}
	for prop2, _ := range r2.Bits {
		if _, ok := r.Bits[prop2]; !ok {
			return errors.Errorf("prop: '%v' in arg, but not receiver", prop2)
		}
	}
	return nil
}

func (h *Handler) Query(r *Request) (*Results, error) {

	results := &Results{
		Bits: make(map[string][]uint64),
	}

	var eg errgroup.Group

	for _, i := range r.Indexes {
		i := i // required for closure
		eg.Go(func() error {
			bits, err := h.queryIndex(i, r)
			if err != nil {
				return err
			}
			results.setIndex(i, bits)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

func (h *Handler) queryIndex(idx string, r *Request) ([]uint64, error) {
	h.sem.Acquire()
	defer h.sem.Release()

	schema, err := h.client.Schema()
	if err != nil {
		return nil, fmt.Errorf("getting schema: %v", err)
	}
	index, err := schema.Index(idx)
	if err != nil {
		return nil, fmt.Errorf("getting index %s from schema: %v", idx, err)
	}

	// Includes.
	var includes []*pilosa.PQLBitmapQuery
	for _, row := range r.Includes {
		frame, err := index.Frame(row.Frame)
		if err != nil {
			return nil, fmt.Errorf("getting frame %s from index %s: %v", row.Frame, idx, err)
		}
		includes = append(includes, frame.Bitmap(row.ID))
	}

	// Excludes.
	var excludes []*pilosa.PQLBitmapQuery
	for _, row := range r.Excludes {
		frame, err := index.Frame(row.Frame)
		if err != nil {
			return nil, fmt.Errorf("getting frame %s from index %s: %v", row.Frame, idx, err)
		}
		excludes = append(excludes, frame.Bitmap(row.ID))
	}

	// Conjuction: Intersect, Union.
	var qry *pilosa.PQLBitmapQuery
	switch r.Conjunction {
	case "and":
		qry = index.Intersect(includes...)
	case "or":
		qry = index.Union(includes...)
	default:
		return nil, fmt.Errorf("invalid conjunction: %s", r.Conjunction)
	}

	// Difference.
	if len(excludes) > 0 {
		var diffArgs []*pilosa.PQLBitmapQuery
		// The first argument to Difference is the conjuction query.
		diffArgs = append(diffArgs, qry)
		for _, e := range excludes {
			diffArgs = append(diffArgs, e)
		}
		qry = index.Difference(diffArgs...)
	}

	// Perform the query.
	response, err := h.client.Query(qry)
	if err != nil {
		return nil, fmt.Errorf("querying index %s: %v", idx, err)
	}

	// Since this isn't a batch query, there should be exactly one result.
	if len(response.ResultList) != 1 {
		return nil, fmt.Errorf("expected 1 result but got %d", len(response.ResultList))
	}
	resp := response.ResultList[0]

	return resp.Bitmap.Bits, nil
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
