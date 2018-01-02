package mindy

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

type Client struct {
	Addr   string
	client *http.Client
}

func (c *Client) Post(r *Request) (*Results, error) {
	if c.client == nil {
		c.client = http.DefaultClient
	}
	bod, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "encoding request")
	}
	resp, err := c.client.Post("http://"+c.Addr+"/mindy", "application/json", bytes.NewBuffer(bod))
	if err != nil {
		return nil, errors.Wrap(err, "making request")
	}
	if resp.StatusCode > 299 {
		return nil, errors.Errorf("unexpected response status code: %d", resp.StatusCode)
	}
	dec := json.NewDecoder(resp.Body)
	res := &Results{}
	err = dec.Decode(res)

	return res, errors.Wrap(err, "decoding response")
}
