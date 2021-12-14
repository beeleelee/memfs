.PHONY: kvfs
kvfs:
	go build -o ./kvfs ./kvdbfs/cmd/main.go 