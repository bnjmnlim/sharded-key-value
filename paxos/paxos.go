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
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // current instance's index into peers[]
  maxInstance int
  instances map[int]AcceptorData
  highestInstanceByPeer map[int]int // Highest known instance value by each peer
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
  if paxos.instances[seq].committedValue != nil {
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

    var cont bool
    cont = true
    majority := (len(paxos.peers)/2)+1  //number needed for majority
    //negmaj := (len(paxos.peers)+1)/2  //number so that majority can no longer be reached

    for cont {
      numAttempt++
      proposerData := ProposerData{paxos.me, 0, -1, nil, 0}
      var vmu sync.Mutex
      var pwg sync.WaitGroup
      pwg.Add(len(paxos.peers))
      vmu.Lock()
      paxos.mu.Lock()
      proposerData.proposalId = (((paxos.instances[seq].higestProposalId / len(paxos.peers)) + 1) * len(paxos.peers)) + paxos.me
      doneMap := paxos.highestInstanceByPeer
      paxos.mu.Unlock()
      vmu.Unlock()

      for i := 0; i < len(paxos.peers); i++ {
        go func(a int) {
          prepareArgs := &PrepareArgs{}
          vmu.Lock()
          prepareArgs.Pnum = proposerData.proposalId
          vmu.Unlock()
          prepareArgs.Seqnum = seq
          prepareArgs.Donemap = doneMap

          var preply PrepareReply

          //fmt.Println("sent to", a)
          ok := call(paxos.peers[a], "Paxos.Prepare", prepareArgs, &preply)
          vmu.Lock()
          if ok {
            if preply.Ok {
              if(preply.PRnum > proposerData.maxProposalId) {
                proposerData.maxProposalId = preply.PRnum
                proposerData.highestAcceptedValue = preply.PRval
              }
              proposerData.numPromises++
            } else {
              if preply.CommitedValue != nil {
                paxos.mu.Lock()
                var tmp = paxos.instances[prepareArgs.Seqnum]
                tmp.committedValue = preply.CommitedValue
                paxos.instances[prepareArgs.Seqnum] = tmp
                //fmt.Println(paxos.me, "value decided for", seq)
                paxos.mu.Unlock()

                //in case of partition need to tell others
                //fmt.Println("notifying others in case of partition")
                if numAttempt > 3 {
                  for y := 0; y < len(paxos.peers); y++ {
                    go func(c int) {
                      cargs := &CommitArgs{}
                      cargs.Seqnum = seq
                      cargs.Value = preply.CommitedValue

                      var creply CommitReply

                      call(paxos.peers[c], "Paxos.Commit", cargs, &creply)
                    }(y)
                  }
                }
              }
            }
            paxos.mu.Lock()
            for k := 0; k < len(paxos.peers); k++ {
              if paxos.highestInstanceByPeer[k] < preply.Donemap[k] {
                paxos.highestInstanceByPeer[k] = preply.Donemap[k]
              }
            }
            paxos.mu.Unlock()
          } else {
            //no response taken as no
          }
          vmu.Unlock()
          pwg.Done()
        }(i) //end of propose thread
      }
      pwg.Wait()

      //done with prepare phase

      if proposerData.numPromises >= majority {
        //fmt.Println("was a majority")
        vmu.Lock()
        var sendval interface{}
        if proposerData.highestAcceptedValue == nil {
          sendval = value
        } else {
          sendval = proposerData.highestAcceptedValue
        }
        vmu.Unlock()
        var awg sync.WaitGroup
        awg.Add(len(paxos.peers))
        for x := 0; x < len(paxos.peers); x++ {
          go func(b int) {
            aargs := &AcceptArgs{}
            aargs.AId = proposerData.proposalId
            aargs.Seqnum = seq
            aargs.Aval = sendval

            var areply AcceptReply

            ok := call(paxos.peers[b], "Paxos.Accept", aargs, &areply)
            vmu.Lock()
            if ok {
              if areply.Ok {
                proposerData.ayes++
              }
            } else {
              //no response taken as no
            }
            vmu.Unlock()
            awg.Done()
          }(x)
        }
        awg.Wait()

        if proposerData.ayes >= majority {

          for y := 0; y < len(paxos.peers); y++ {
            go func(c int) {
              cargs := &CommitArgs{}
              cargs.Seqnum = seq
              cargs.Value = sendval

              var creply CommitReply

              call(paxos.peers[c], "Paxos.Commit", cargs, &creply)
            }(y)
          }

          cont = false
          paxos.mu.Lock()
          if paxos.instances[seq].committedValue == nil {
            //fmt.Println("disruption had to set for self")
            temp := paxos.instances[seq]
            temp.committedValue = sendval
            paxos.instances[seq] = temp
          }
          paxos.mu.Unlock()
          //fmt.Println(paxos.me, "Value decided. instance:", seq)
        }
      } else {
        //what to do if majority was not reached
        paxos.mu.Lock()
        if paxos.dead || paxos.instances[seq].committedValue != nil {
          cont = false
        }
        paxos.mu.Unlock()
        time.Sleep(3 * time.Millisecond)
      }

    }
    //fmt.Println("done", seq)
  }() //end of Start thread
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
  //fmt.Println("min called, min for", paxos.me, "is", (min+1))
  return (min+1)
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
  if paxos.instances[seq].committedValue != nil {
    return true, paxos.instances[seq].committedValue
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
  _, ok := paxos.instances[args.Seqnum]
  if !ok {
    paxos.instances[args.Seqnum] = AcceptorData{0, 0, nil, nil}
    if args.Seqnum > paxos.maxInstance {
      paxos.maxInstance = args.Seqnum
    }
  }

  if args.Pnum > paxos.instances[args.Seqnum].higestProposalId && paxos.instances[args.Seqnum].committedValue == nil {
    var tmp = paxos.instances[args.Seqnum]
    tmp.higestProposalId = args.Pnum
    paxos.instances[args.Seqnum] = tmp
    reply.Ok = true
    reply.PRnum = paxos.instances[args.Seqnum].acceptedProposalId
    reply.PRval = paxos.instances[args.Seqnum].acceptedValue
  } else {
    reply.Ok = false
    reply.CommitedValue = paxos.instances[args.Seqnum].committedValue
  }

  for k := 0; k < len(paxos.peers); k++ {
    if paxos.highestInstanceByPeer[k] < args.Donemap[k] {
      paxos.highestInstanceByPeer[k] = args.Donemap[k]
    }
    //fmt.Println(paxos.me, "Map updated", k, "to", paxos.highestInstanceByPeer[k] )
  }
  reply.Donemap = paxos.highestInstanceByPeer
  return nil
}

//handler for acceptor for accept
func (paxos *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  paxos.mu.Lock()
  defer paxos.mu.Unlock()
  if args.AId >= paxos.instances[args.Seqnum].higestProposalId {
    var tmp = paxos.instances[args.Seqnum]
    tmp.higestProposalId = args.AId
    tmp.acceptedProposalId = args.AId
    tmp.acceptedValue = args.Aval
    paxos.instances[args.Seqnum] = tmp
    reply.Ok = true
    reply.ARnum = args.AId
  } else {
    reply.Ok = false
    reply.ARnum = paxos.instances[args.Seqnum].higestProposalId
  }
  return nil
}

//handler for commit
func (paxos *Paxos) Commit(args *CommitArgs, reply *CommitReply) error {
  paxos.mu.Lock()
  tmp := paxos.instances[args.Seqnum]
  tmp.committedValue = args.Value
  paxos.instances[args.Seqnum] = tmp
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
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    paxos.l = l

    // create a thread to accept RPC connections
    go func() {
      for paxos.dead == false {
        conn, err := paxos.l.Accept()
        if err == nil && paxos.dead == false {
          if paxos.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if paxos.unreliable && (rand.Int63() % 1000) < 200 {
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
