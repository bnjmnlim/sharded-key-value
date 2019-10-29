package paxos

type PrepareArgs struct {
	SeqNum     int
	ProposalId int
	DoneMap    map[int]int
}

type PrepareReply struct {
	Ok             bool
	MaxProposalId  int
	AcceptedPId    int
	AcceptedValue  interface{}
	CommittedValue interface{}
	DoneMap        map[int]int
}

type AcceptArgs struct {
	ProposalId int
	Value      interface{}
	SeqNum     int
}

type AcceptReply struct {
	Ok    bool
	ARnum int
}

type CommitArgs struct {
	SeqNum int
	Value  interface{}
}

type CommitReply struct {
}

type ProposerData struct {
	ProposalId          int         //decided num
	NumPromises         int         //number of prepare_ok received
	HighestSeenPId      int         //largest seen Proposal ID from acceptors
	MaxAcceptedPId      int         //highest num from prepare
	MaxPIdAcceptedValue interface{} //value of highest num from prepare
	NumAccepts          int         //number of accept_ok received
}

type AcceptorData struct {
	AcceptedProposalId int
	HigestProposalId   int
	AcceptedValue      interface{}
	CommittedValue     interface{}
}
