package utils

import (
	"fmt"
	"log"
)

type Connection string 
const (
	SERVER Connection = "SERVER"
	CLIENT Connection = "CLIENT"
)

type EvType string 
const (
	BROADCAST EvType = "BROADCAST"
	ERROR EvType = "ERROR"
	JOIN EvType = "JOIN"
	LEAVE EvType = "LEAVE"
	MESSAGE_RECEIVED EvType = "MESSAGE_RECEIVED"
	SHUTDOWN EvType = "SHUTDOWN"
	STREAM_END EvType = "STREAM_END"
)

func LogMessage(id, timestp int, component Connection, event EvType, msg string) {
	log.SetFlags(0)
	var logFormat string 
	if component == SERVER {
		logFormat = fmt.Sprintf("[%v]:[%v]:[%d]/ %s", component, event, timestp, msg)
	} else {
		logFormat = fmt.Sprintf("[%v-%d]:[%v]:[%d]/ %s", component, id, event, timestp, msg)
	}

	if event == ERROR {
		log.Fatal(logFormat)
		return
	}

	log.Print(logFormat)
}