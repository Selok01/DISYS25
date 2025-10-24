.PHONY: run

CLIENTS ?= 3

run:
	@bash -c 'cd server && go run main.go & \
		SERVER_PID=$$!; \
		sleep 1; \
		for i in $$(seq 1 $(CLIENTS)); do \
			(cd clients && go run main.go) & \
			sleep 1; \
		done; \
		wait; \
		kill $$SERVER_PID'