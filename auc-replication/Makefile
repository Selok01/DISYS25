.PHONY: run

CLIENTS ?= 3
SERVERS ?= 2

run:
	@bash -c 'cd servers && go run main.go 2>&1 & \
		sleep 1; \
		for i in $$(seq 1 $(CLIENTS)); do \
			(cd clients && go run main.go 2>&1) & \
			sleep 2; \
		done; \
		trap '"'"'kill -TERM $$(lsof -t -i:8080) 2>/dev/null'"'"' SIGINT; \
		wait;' \
