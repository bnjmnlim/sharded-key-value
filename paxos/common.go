package paxos

type PrepareArgs struct {
	Seqnum  int
	Pnum    int
	Donemap map[int]int
}

type PrepareReply struct {
	Ok            bool
	PRnum         int
	PRval         interface{}
	CommitedValue interface{}
	Donemap       map[int]int
}

type AcceptArgs struct {
	AId    int
	Aval   interface{}
	Seqnum int
}

type AcceptReply struct {
	Ok    bool
	ARnum int
}

type CommitArgs struct {
	Seqnum int
	Value  interface{}
}

type CommitReply struct {
}

type ProposerData struct {
	proposalId           int         //decided num
	numPromises          int         //number of prepare_ok received
	maxProposalId        int         //highest num from prepare
	highestAcceptedValue interface{} //value of highest num from prepare
	ayes                 int         //number of accept_ok received
}

type AcceptorData struct {
	acceptedProposalId int
	higestProposalId   int
	acceptedValue      interface{}
	committedValue     interface{}
}
