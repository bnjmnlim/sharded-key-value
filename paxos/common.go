package paxos

type PrepareArgs struct {
	seqNum     int
	proposalId int
	doneMap    map[int]int
}

type PrepareReply struct {
	Ok             bool
	maxProposalId  int
	acceptedPId    int
	acceptedValue  interface{}
	committedValue interface{}
	doneMap        map[int]int
}

type AcceptArgs struct {
	proposalId int
	value      interface{}
	seqNum     int
}

type AcceptReply struct {
	Ok    bool
	ARnum int
}

type CommitArgs struct {
	seqNum int
	Value  interface{}
}

type CommitReply struct {
}

type ProposerData struct {
	proposalId          int         //decided num
	numPromises         int         //number of prepare_ok received
	highestSeenPId      int         //largest seen Proposal ID from acceptors
	maxAcceptedPId      int         //highest num from prepare
	maxPIdAcceptedValue interface{} //value of highest num from prepare
	numAccepts          int         //number of accept_ok received
}

type AcceptorData struct {
	acceptedProposalId int
	higestProposalId   int
	acceptedValue      interface{}
	committedValue     interface{}
}
