package test

import "flag"

var FlightPort string

func init() {
	flag.StringVar(&FlightPort, "port", "4082", "flight connection port, default 4082")
}
