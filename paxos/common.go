package paxos

type PrepareArgs struct {
	SeqNum     int         //sequence number. Each sequence is an individual (possibly concurrent) instance of paxos
	ProposalId int         //proposal ID
	DoneMap    map[int]int //map of lowest known sequence number for each node. Used to truncate log
}

type PrepareReply struct {
	Ok             bool        //whether promise has been made
	MaxProposalId  int         //highest proposal ID seen by the acceptor, hint for proposer
	AcceptedPId    int         //PId of last value acceptor has accepted
	AcceptedValue  interface{} //value of last value acceptor has accepted
	CommittedValue interface{} //committed value, if sequence has been commited already
	DoneMap        map[int]int //map of lowest known sequence number for each node. Used to truncate log
}

type AcceptArgs struct {
	ProposalId int         //proposal ID of accept request
	Value      interface{} //value to accept
	SeqNum     int         //sequence of paxos to accept for
}

type AcceptReply struct {
	Ok            bool //whether value has been accepted
	MaxProposalId int  //highest proposal ID the acceptor has seen, if reply ok is false
}

type CommitArgs struct {
	SeqNum int         //sequence for value to commit
	Value  interface{} //value to commit
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
	AcceptedProposalId int         //proposal ID an acceptor last accepted
	HigestProposalId   int         //largest proposal ID this acceptor has seen
	AcceptedValue      interface{} //value an acceptor last accepted
	CommittedValue     interface{} //value that has been committed
}
