.PHONY: run

CLIENTS ?= 3

run:
	@bash -c 'cd server && go run main.go > server.log 2>&1 & \
		sleep 1; \
		for i in $$(seq 1 $(CLIENTS)); do \
			(cd clients && go run main.go >"$${i}_client.log" 2>&1) & \
			sleep 2; \
		done; \
		trap '"'"'kill -TERM $$(lsof -t -i:8080) 2>/dev/null'"'"' SIGINT; \
		wait; \
		cat ./server/server.log ./clients/*.log'
