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
	"net/http"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Main struct {
	Pilosa []string `help:"Comma separated list of pilosa hosts/ports."`
	Bind   string   `help:"Host/port to bind to."`
}

func NewMain() *Main {
	return &Main{
		Pilosa: []string{"localhost:10101"},
		Bind:   ":10001",
	}
}

func (m *Main) Run() error {
	client, err := pilosa.NewClientFromAddresses(m.Pilosa, nil)
	if err != nil {
		return errors.Wrap(err, "creating Pilosa client")
	}
	h := &Handler{client: client}

	sm := http.NewServeMux()
	sm.HandleFunc("/mindy", h.handleMindy)
	s := &http.Server{
		Addr:    m.Bind,
		Handler: sm,
	}

	return s.ListenAndServe()
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

type Handler struct {
	client *pilosa.Client
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

	results, err := Query(req)
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
}

func Query(r *Request) (*Results, error) {
	return nil, errors.New("Query unimplemented")
}
