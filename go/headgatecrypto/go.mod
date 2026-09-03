module github.com/mujhtech/headgate/go/headgatecrypto

go 1.27.0

require github.com/mujhtech/headgate/go v0.1.7

require (
	github.com/mujhtech/headgate/go/headgatetest v0.1.7 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/mujhtech/headgate/go => ..

replace github.com/mujhtech/headgate/go/headgatetest => ../headgatetest

replace github.com/mujhtech/headgate/go/headgatemigrate => ../headgatemigrate
