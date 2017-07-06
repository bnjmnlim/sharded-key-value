package paxos

type PrepareArgs struct {
  Seqnum int
  Pnum int
  Donemap map[int]int
}

type PrepareReply struct {
  Ok bool
  PRnum int
  PRval interface{}
  CommitedValue interface{}
  Donemap map[int]int
}

type AcceptArgs struct {
  AId int
  Aval interface{}
  Seqnum int
}

type AcceptReply struct {
  Ok bool
  ARnum int
}

type CommitArgs struct {
  Seqnum int
  Value interface{}
}

type CommitReply struct {

}

type PropVar struct {  //struct containing variables for proposers
  decidednum int  //decided num
  pyes int  //number of prepare_ok received
  highN int  //highest num from prepare
  highV interface{}  //value of highest num from prepare
  ayes int  //number of accept_ok received
}

type AccVar struct {
  highA int
  highP int
  highV interface{}
  CValue interface{}
}