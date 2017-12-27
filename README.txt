Mindy - a Multi-INDex proxY.

Get:
go get -u github.com/pilosa/mindy/cmd/mindy

Build:
go install github.com/pilosa/mindy/cmd/mindy

Running:
mindy -h

Use:
Mindy starts a server at (by default) localhost:10001. It accepts queries
of the form mindy.Request (in json), then constructs a Pilosa query, queries
Pilosa, and returns the results in the form mindy.Results (also json).

Example:
Running mindy with defaults, one might submit a request as follows:

curl -XPOST localhost:10001/mindy -d '{"indexes":["1"], "includes":[{"frame":"a","id":1}], "excludes":[], "conjunction":"and"}'

