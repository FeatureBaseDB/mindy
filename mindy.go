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
	"io"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

// Main holds the exported options and some unexported state for mindy.
type Main struct {
	Pilosa      []string `help:"Comma separated list of pilosa hosts/ports."`
	Bind        string   `help:"Host/port to bind to."`
	Concurrency int      `help:"Maximum number of simultaneous Pilosa requests."`
	s           *http.Server
	ln          net.Listener
}

// NewMain returns a Main with the default options.
func NewMain() *Main {
	return &Main{
		Pilosa:      []string{"localhost:10101"},
		Bind:        ":10001",
		Concurrency: 2,
	}
}

// Run starts the mindy server and only returns if there is an error.
func (m *Main) Run() error {
	err := m.listen()
	if err != nil {
		return errors.Wrap(err, "Main.listen")
	}
	err = m.serve()
	return errors.Wrap(err, "Main.serve")
}

// serve starts mindy's http server - it does not return unless there is an error.
func (m *Main) serve() error {
	return m.s.Serve(tcpKeepAliveListener{m.ln.(*net.TCPListener)})
}

// listen calls listen on the bind port so that the OS will accept new
// connections. It returns immediately.
func (m *Main) listen() error {
	client, err := pilosa.NewClientFromAddresses(m.Pilosa, nil)
	if err != nil {
		return errors.Wrap(err, "creating Pilosa client")
	}
	h := &Handler{
		client: client,
		sem:    make(semaphore, m.Concurrency), // length of semaphore is number of concurrent goroutines querying pilosa.
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

// Request defines the structure of a request to mindy.
type Request struct {
	Indexes     []string `json:"indexes"`
	Includes    []Row    `json:"includes"`
	Excludes    []Row    `json:"excludes"`
	Conjunction string   `json:"conjunction"`
}

// Row specifies a single Pilosa row (given an index name).
type Row struct {
	ID    uint64 `json:"id"`
	Frame string `json:"frame"`
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

	err = h.Query(w, req)
	if err != nil {
		http.Error(w, "querying pilosa: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) Query(w io.Writer, r *Request) error {
	maxSlices, err := h.SlicesMax()
	if err != nil {
		return errors.Wrap(err, "getting max slices")
	}

	schema, err := h.client.Schema()
	if err != nil {
		return fmt.Errorf("getting schema: %v", err)
	}

	results := make(chan Bit, 0)

	var eg errgroup.Group
	for _, i := range r.Indexes {
		qry, err := buildQuery(schema, i, r)
		if err != nil {
			return errors.Wrap(err, "building query")
		}

		maxSlice, ok := maxSlices[i]
		if !ok {
			return errors.Errorf("index '%v' not found in max slices", i)
		}
		for sl := uint64(0); sl <= maxSlice; sl++ {
			sl := sl // necessary since it's used in closure below.
			eg.Go(func() error {
				return h.sliceQuery(qry, sl, results)
			})
		}
	}

	// read results off channel and write out.
	writeErr := make(chan error, 1)
	go func() {
		defer func() {
			close(writeErr) // TODO: need this closure?
		}()
		for bit := range results {
			_, err := w.Write([]byte(fmt.Sprintf("%s,%d\n", bit.Index, bit.Col)))
			if err != nil {
				writeErr <- errors.Wrap(err, "writing Bit to output")
				return
			}
		}
	}()

	// wait for sliceQuery routines to finish.
	err = eg.Wait()
	// close results so that the result reader can finish.
	close(results)
	if err != nil {
		return err
	}

	// return any error during writing of results.
	return <-writeErr
}

// sliceQuery executes qry against a single slice in Pilosa.
func (h *Handler) sliceQuery(qry *pilosa.PQLBitmapQuery, slice uint64, results chan<- Bit) error {
	h.sem.Acquire()
	response, err := h.client.Query(qry, pilosa.Slices(slice))
	h.sem.Release()
	if err != nil {
		return fmt.Errorf("querying index %s: %v", qry.Index().Name(), err)
	}
	// Since this isn't a batch query, there should be exactly one result.
	if len(response.ResultList) != 1 {
		return fmt.Errorf("expected 1 result but got %d", len(response.ResultList))
	}
	resp := response.ResultList[0]
	for _, bit := range resp.Bitmap.Bits {
		results <- Bit{Index: qry.Index().Name(), Col: bit}
	}
	return nil
}

// Bit is a single column in Pilosa. The response to a /mindy request is a
// stream of these (in index,col CSV format).
type Bit struct {
	Index string
	Col   uint64
}

// buildQuery constructs a Pilosa query from the Request.
func buildQuery(schema *pilosa.Schema, idx string, r *Request) (*pilosa.PQLBitmapQuery, error) {
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
	return qry, nil
}

// SlicesMax returns a map with keys of all indexes in Pilosa, and values of the
// maximum slice in that index. This method may(TODO) cache the response for a
// short time to avoid many redundant requests to Pilosa for this information.
func (h *Handler) SlicesMax() (map[string]uint64, error) {
	_, data, err := h.client.HttpRequest("GET", "/slices/max", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "slices max request")
	}
	sm := struct {
		SlicesMax map[string]uint64 `json:"maxSlices"`
	}{}
	err = json.Unmarshal(data, &sm)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling slices max")
	}
	return sm.SlicesMax, nil
}

// tcpKeepAliveListener is used to replicate the functionality of
// net/http.ListenAndServe while retaining the capability of separating the call
// to Listen from calling serve which is useful for avoiding race conditions in
// tests.
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
