module github.com/mujhtech/headgate/go/headgateprometheus

go 1.27.0

require (
	github.com/mujhtech/headgate/go v0.1.8
	github.com/prometheus/client_golang v1.24.1
	github.com/prometheus/client_model v0.6.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mujhtech/headgate/go/headgatetest v0.1.8 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/mujhtech/headgate/go => ..

replace github.com/mujhtech/headgate/go/headgatemigrate => ../headgatemigrate

replace github.com/mujhtech/headgate/go/headgatetest => ../headgatetest
