package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {

	log.Printf(format, a...)

	return
}

func Max(a int ,b int )(int){
	if a>b{
		return a
	}else{
		return b
	}
}