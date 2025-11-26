# Auction System with Replication (use instructions)

## 1. Getting ready

Compile `auc_replication.proto` to generate the Go files that let us build our gRPC peer node instances.  

From the root folder (`DISYS25/auc-replication/`) run the following command on your terminal: 

`cd grpc && protoc --go_out=. --go-grpc_out=. auc_replication.proto`

Lastly, to make sure you won't be getting compatibility errors with go modules by running: `go mod tidy`

## 2. Running the system

Running our implementation of the auction system can be done in two simple steps: 

First, go back to the root folder by running: `cd ..`

Next, run: `make run`

Additionally, you can run the auction system with as many participants (`n`) as you want, by simply modifying the command above like this: `make run CLIENTS=n`

> **Note:** You may need to also give permission to the bash script the `make run` calls when running the system for the first time. To do so, simply run (also from the root folder of the project): `chmod +x ./create_network.sh`

Whenever you are ready to simulate the scenario where the Leader node crashes, open a separate terminal and (again from the root folder of this project), run: `make kill-leader`

> **Note:** It is also possible to make the Follower replica crash by running: `make kill-follower`. However, this wouldn't have any impact in the normal execution of the system, so it's kind of pointless to do so.

## 3. Checking logs and cleaning up

Once you start running the system, your terminal won't display any logs. This is because we print logs for each instance of the system (servers and clients) in their respective log file. 

You can inspect them in real time by looking at either the `servers` or `clients` folder, and opening as many log files as you're interested in from those folders.

You can simply interrupt the system's execution when you think you have enough logs to inspect. 

> **Note:** After a run, you will have to run `make clean` if you wish to run the progam again. This will kill all opened ports making them available again, and remove the run-specific created log files.
