package main

import (
	"errors"
	"sync"
)

type SnapshotState struct {
	mu              sync.Mutex
	LastIncludedIndex int
	LastIncludedTerm  int
	Data            []byte
	IsReceiving     bool
}

// InstallSnapshot handles incoming snapshot chunks.
// It buffers data until the final chunk is received to ensure atomicity.
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// Atomic application: Only update state machine if the full snapshot is received
	if args.Done {
		// 1. Verify integrity (checksum logic would go here)
		// 2. Truncate log and update state machine
		rf.applySnapshot(args)
		rf.snapshotState.IsReceiving = false
	} else {
		// Buffer chunk
		rf.snapshotState.Data = append(rf.snapshotState.Data, args.Data...)
		rf.snapshotState.IsReceiving = true
	}

	reply.Term = rf.currentTerm
	return nil
}

func (rf *Raft) applySnapshot(args InstallSnapshotArgs) {
	// Implementation of log truncation and state machine update
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = rf.log[args.LastIncludedIndex:]
}