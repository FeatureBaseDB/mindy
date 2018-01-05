package mindy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

type Client struct {
	Addr   string
	client *http.Client
}

func (c *Client) Post(r *Request) (*bufio.Scanner, error) {
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
		bod, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("unexpected response status code: %d. body: %v", resp.StatusCode, string(bod))
	}
	bs := bufio.NewScanner(resp.Body)
	return bs, nil
}
