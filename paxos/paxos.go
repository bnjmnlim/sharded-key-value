package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// paxos = paxos.Make(peers []string, me string)
// paxos.Start(seq int, v interface{}) -- start agreement on new instance
// paxos.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// paxos.Done(seq int) -- ok to forget all instances <= seq
// paxos.Max() int -- highest instance seq known, or -1
// paxos.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type Paxos struct {
	mu                    sync.Mutex
	l                     net.Listener
	dead                  bool
	unreliable            bool
	rpcCount              int
	peers                 []string
	me                    int // current instance's index into peers[]
	maxInstance           int
	instances             map[int]AcceptorData
	highestInstanceByPeer map[int]int // Highest known committed instance by each peer
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// use call() to send all RPCs, in client.go and server.go.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (paxos *Paxos) Start(seq int, value interface{}) {
	paxos.mu.Lock()
	if paxos.instances[seq].CommittedValue != nil {
		return
	}
	paxos.mu.Unlock()
	go func() {
		numAttempt := 0
		paxos.mu.Lock()
		if seq > paxos.maxInstance {
			paxos.maxInstance = seq
		}
		paxos.instances[seq] = AcceptorData{0, 0, nil, nil}
		paxos.mu.Unlock()
		proposerData := ProposerData{0, 0, 0, 0, nil, 0}
		var proposerDataLock sync.Mutex

		var cont bool
		cont = true
		majority := (len(paxos.peers) / 2) + 1 //number needed for majority

		for cont {
			numAttempt++

			var proposeWaitGroup sync.WaitGroup
			proposeWaitGroup.Add(len(paxos.peers))
			proposerDataLock.Lock()
			paxos.mu.Lock()
			proposerData.ProposalId = (((proposerData.HighestSeenPId / len(paxos.peers)) + 1) * len(paxos.peers)) + paxos.me
			proposerData.NumAccepts = 0
			doneMap := paxos.highestInstanceByPeer
			paxos.mu.Unlock()
			proposerDataLock.Unlock()

			for i := 0; i < len(paxos.peers); i++ {
				go func(a int) {
					prepareArgs := &PrepareArgs{}
					proposerDataLock.Lock()
					prepareArgs.ProposalId = proposerData.ProposalId
					proposerDataLock.Unlock()
					prepareArgs.SeqNum = seq
					prepareArgs.DoneMap = doneMap

					var preply PrepareReply

					ok := call(paxos.peers[a], "Paxos.Prepare", prepareArgs, &preply)
					proposerDataLock.Lock()
					if ok {
						if preply.Ok {
							if preply.AcceptedPId > proposerData.MaxAcceptedPId {
								proposerData.MaxAcceptedPId = preply.AcceptedPId
								proposerData.MaxPIdAcceptedValue = preply.AcceptedValue
							}
							proposerData.NumPromises++
						} else {
							if preply.CommittedValue != nil {
								paxos.mu.Lock()
								var temp = paxos.instances[prepareArgs.SeqNum]
								temp.CommittedValue = preply.CommittedValue
								paxos.instances[prepareArgs.SeqNum] = temp
								paxos.mu.Unlock()

								//Notifying other nodes of committed value so they can give up
								//Not necessary, but can help if there is a network partition
								if numAttempt > 3 {
									for y := 0; y < len(paxos.peers); y++ {
										go func(c int) {
											cargs := &CommitArgs{}
											cargs.SeqNum = seq
											cargs.Value = preply.CommittedValue

											var creply CommitReply

											call(paxos.peers[c], "Paxos.Commit", cargs, &creply)
										}(y)
									}
								}
							} else {
								if preply.MaxProposalId > proposerData.HighestSeenPId {
									proposerData.HighestSeenPId = preply.MaxProposalId
								}
							}
						}
						paxos.mu.Lock()
						for k := 0; k < len(paxos.peers); k++ {
							if paxos.highestInstanceByPeer[k] < preply.DoneMap[k] {
								paxos.highestInstanceByPeer[k] = preply.DoneMap[k]
							}
						}
						paxos.mu.Unlock()
					} //no response taken as no
					proposerDataLock.Unlock()
					proposeWaitGroup.Done()
				}(i) //end of propose thread
			}
			proposeWaitGroup.Wait()
			//done with prepare phase

			if proposerData.NumPromises >= majority {
				proposerDataLock.Lock()
				var sendval interface{}
				if proposerData.MaxPIdAcceptedValue == nil {
					sendval = value
				} else {
					sendval = proposerData.MaxPIdAcceptedValue
				}
				proposerDataLock.Unlock()
				var acceptWaitGroup sync.WaitGroup
				acceptWaitGroup.Add(len(paxos.peers))
				for x := 0; x < len(paxos.peers); x++ {
					go func(index int) {
						acceptArgs := &AcceptArgs{}
						acceptArgs.ProposalId = proposerData.ProposalId
						acceptArgs.SeqNum = seq
						acceptArgs.Value = sendval

						var acceptReply AcceptReply

						ok := call(paxos.peers[index], "Paxos.Accept", acceptArgs, &acceptReply)
						proposerDataLock.Lock()
						if ok {
							if acceptReply.Ok {
								proposerData.NumAccepts++
							} else {
								if acceptReply.MaxProposalId > proposerData.HighestSeenPId {
									proposerData.HighestSeenPId = acceptReply.MaxProposalId
								}
							}
						} //no response taken as no
						proposerDataLock.Unlock()
						acceptWaitGroup.Done()
					}(x)
				}
				acceptWaitGroup.Wait()

				if proposerData.NumAccepts >= majority {

					for y := 0; y < len(paxos.peers); y++ {
						go func(c int) {
							cargs := &CommitArgs{}
							cargs.SeqNum = seq
							cargs.Value = sendval
							var creply CommitReply

							call(paxos.peers[c], "Paxos.Commit", cargs, &creply)
						}(y)
					}

					cont = false
					paxos.mu.Lock()
					if paxos.instances[seq].CommittedValue == nil {
						temp := paxos.instances[seq]
						temp.CommittedValue = sendval
						paxos.instances[seq] = temp
					}
					paxos.mu.Unlock()
				}
			} else {
				//If not enough promises were recieved...
				paxos.mu.Lock()
				if paxos.dead || paxos.instances[seq].CommittedValue != nil {
					cont = false
				}
				paxos.mu.Unlock()
				time.Sleep(3 * time.Millisecond)
			}

		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (paxos *Paxos) Done(seq int) {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	if paxos.highestInstanceByPeer[paxos.me] < seq {
		paxos.highestInstanceByPeer[paxos.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (paxos *Paxos) Max() int {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	return paxos.maxInstance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (paxos *Paxos) Min() int {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	var min int
	elem, ok := paxos.highestInstanceByPeer[0]
	if !ok {
		return 0
	} else {
		min = elem
	}
	for i := 1; i < len(paxos.peers); i++ {
		elem, ok = paxos.highestInstanceByPeer[i]
		if !ok {
			return 0
		} else {
			if elem < min {
				min = elem
			}
		}
	}
	for x := 0; x <= min; x++ {
		delete(paxos.instances, x)
	}
	return (min + 1)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (paxos *Paxos) Status(seq int) (bool, interface{}) {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	if paxos.instances[seq].CommittedValue != nil {
		return true, paxos.instances[seq].CommittedValue
	}
	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (paxos *Paxos) Kill() {
	paxos.dead = true
	if paxos.l != nil {
		paxos.l.Close()
	}
}

//handler for acceptor for prepare
func (paxos *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	_, ok := paxos.instances[args.SeqNum]
	if !ok {
		paxos.instances[args.SeqNum] = AcceptorData{0, 0, nil, nil}
		if args.SeqNum > paxos.maxInstance {
			paxos.maxInstance = args.SeqNum
		}
	}

	if paxos.instances[args.SeqNum].CommittedValue != nil {
		reply.Ok = false
		reply.CommittedValue = paxos.instances[args.SeqNum].CommittedValue
	} else {
		if args.ProposalId > paxos.instances[args.SeqNum].HigestProposalId {
			acceptorData := paxos.instances[args.SeqNum]
			acceptorData.HigestProposalId = args.ProposalId
			paxos.instances[args.SeqNum] = acceptorData
			reply.Ok = true
			reply.AcceptedPId = acceptorData.AcceptedProposalId
			reply.AcceptedValue = acceptorData.AcceptedValue
		} else {
			reply.Ok = false
			reply.MaxProposalId = paxos.instances[args.SeqNum].HigestProposalId
		}
	}

	for k := 0; k < len(paxos.peers); k++ {
		if paxos.highestInstanceByPeer[k] < args.DoneMap[k] {
			paxos.highestInstanceByPeer[k] = args.DoneMap[k]
		}
	}
	reply.DoneMap = paxos.highestInstanceByPeer
	return nil
}

//handler for acceptor for accept
func (paxos *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	paxos.mu.Lock()
	defer paxos.mu.Unlock()
	if args.ProposalId >= paxos.instances[args.SeqNum].HigestProposalId {
		var acceptorData = paxos.instances[args.SeqNum]
		acceptorData.HigestProposalId = args.ProposalId
		acceptorData.AcceptedProposalId = args.ProposalId
		acceptorData.AcceptedValue = args.Value
		paxos.instances[args.SeqNum] = acceptorData
		reply.Ok = true
	} else {
		reply.Ok = false
		reply.MaxProposalId = paxos.instances[args.SeqNum].HigestProposalId
	}
	return nil
}

//handler for commit
func (paxos *Paxos) Commit(args *CommitArgs, reply *CommitReply) error {
	paxos.mu.Lock()
	tmp := paxos.instances[args.SeqNum]
	tmp.CommittedValue = args.Value
	paxos.instances[args.SeqNum] = tmp
	paxos.mu.Unlock()
	return nil
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	paxos := &Paxos{}
	paxos.peers = peers
	paxos.me = me
	paxos.maxInstance = -1
	paxos.instances = make(map[int]AcceptorData)
	paxos.highestInstanceByPeer = make(map[int]int)
	for i := 0; i < len(paxos.peers); i++ {
		paxos.highestInstanceByPeer[i] = -1
	}

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(paxos)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(paxos)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		paxos.l = l

		// create a thread to accept RPC connections
		go func() {
			for paxos.dead == false {
				conn, err := paxos.l.Accept()
				if err == nil && paxos.dead == false {
					if paxos.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if paxos.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						paxos.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						paxos.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && paxos.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return paxos
}
