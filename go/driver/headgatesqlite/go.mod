module github.com/mujhtech/headgate/go/driver/headgatesqlite

go 1.27.0

require (
	github.com/mujhtech/headgate/go v0.1.10
	github.com/ncruces/go-sqlite3 v0.35.4
)

require (
	github.com/mujhtech/headgate/go/headgatetest v0.1.10 // indirect
	github.com/ncruces/go-sqlite3-wasm/v5 v5.0.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/mujhtech/headgate/go => ../..

replace github.com/mujhtech/headgate/go/headgatetest => ../../headgatetest
