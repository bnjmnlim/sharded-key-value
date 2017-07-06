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
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
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
  me int // index into peers[]
  maxinst int
  instances map[int]AccVar
  doneval map[int]int

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
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  if px.instances[seq].CValue != nil {
  	return
  }
  px.mu.Unlock()
  //fmt.Println(px.me, "started seq", seq)
  go func() {
    numattempt := 0
    px.mu.Lock()
    if seq > px.maxinst {
      px.maxinst = seq
    }
    px.instances[seq] = AccVar{-1, 0, nil, nil}
    px.mu.Unlock()

    var cont bool
    cont = true
    majority := (len(px.peers)/2)+1  //number needed for majority
    //negmaj := (len(px.peers)+1)/2  //number so that majority can no longer be reached

    for cont {
      numattempt++
      //fmt.Println(px.me, "trying instance", seq, "again")
      vars := PropVar{px.me, 0, -1, nil, 0}
      var vmu sync.Mutex
      var pwg sync.WaitGroup
      pwg.Add(len(px.peers))
      vmu.Lock()
      px.mu.Lock()
      vars.decidednum = (((px.instances[seq].highP / len(px.peers)) + 1) * len(px.peers)) + px.me
      maptosend := px.doneval
      px.mu.Unlock()
      vmu.Unlock()

      for i := 0; i < len(px.peers); i++ {
        go func(a int) {
          pargs := &PrepareArgs{}
          vmu.Lock()
          pargs.Pnum = vars.decidednum
          vmu.Unlock()
          pargs.Seqnum = seq
          pargs.Donemap = maptosend

          var preply PrepareReply

          //fmt.Println("sent to", a)
          ok := call(px.peers[a], "Paxos.Prepare", pargs, &preply)
          vmu.Lock()
          if ok {
            if preply.Ok {
              if(preply.PRnum > vars.highN) {
                vars.highN = preply.PRnum
                vars.highV = preply.PRval
              }
              vars.pyes++
            } else {
              if preply.CommitedValue != nil {
                px.mu.Lock()
                var tmp = px.instances[pargs.Seqnum]
                tmp.CValue = preply.CommitedValue
                px.instances[pargs.Seqnum] = tmp
                //fmt.Println(px.me, "value decided for", seq)
                px.mu.Unlock()

                //in case of partition need to tell others
                //fmt.Println("notifying others in case of partition")
                if numattempt > 3 {
                  for y := 0; y < len(px.peers); y++ {
                    go func(c int) {
                      cargs := &CommitArgs{}
                      cargs.Seqnum = seq
                      cargs.Value = preply.CommitedValue

                      var creply CommitReply

                      call(px.peers[c], "Paxos.Commit", cargs, &creply)
                    }(y)
                  }
                }
              }
            }
            px.mu.Lock()
            for k := 0; k < len(px.peers); k++ {
              if px.doneval[k] < preply.Donemap[k] {
                px.doneval[k] = preply.Donemap[k]
              }
            }
            px.mu.Unlock()
          } else {
            //no response taken as no
          }
          vmu.Unlock()
          pwg.Done()
        }(i) //end of propose thread
      }
      pwg.Wait()

      //done with prepare phase

      if vars.pyes >= majority {
        //fmt.Println("was a majority")
        vmu.Lock()
        var sendval interface{}
        if vars.highV == nil {
          sendval = v
        } else {
          sendval = vars.highV
        }
        vmu.Unlock()
        var awg sync.WaitGroup
        awg.Add(len(px.peers))
        for x := 0; x < len(px.peers); x++ {
          go func(b int) {
            aargs := &AcceptArgs{}
            aargs.AId = vars.decidednum
            aargs.Seqnum = seq
            aargs.Aval = sendval

            var areply AcceptReply

            ok := call(px.peers[b], "Paxos.Accept", aargs, &areply)
            vmu.Lock()
            if ok {
              if areply.Ok {
                vars.ayes++
              }
            } else {
              //no response taken as no
            }
            vmu.Unlock()
            awg.Done()
          }(x)
        }
        awg.Wait()

        if vars.ayes >= majority {

          for y := 0; y < len(px.peers); y++ {
            go func(c int) {
              cargs := &CommitArgs{}
              cargs.Seqnum = seq
              cargs.Value = sendval

              var creply CommitReply

              call(px.peers[c], "Paxos.Commit", cargs, &creply)
            }(y)
          }

          cont = false
          px.mu.Lock()
          if px.instances[seq].CValue == nil {
            //fmt.Println("disruption had to set for self")
            temp := px.instances[seq]
            temp.CValue = sendval
            px.instances[seq] = temp
          }
          px.mu.Unlock()
          //fmt.Println(px.me, "Value decided. instance:", seq)
        }
      } else {
        //what to do if majority was not reached
        px.mu.Lock()
        if px.dead || px.instances[seq].CValue != nil {
          cont = false
        }
        px.mu.Unlock()
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
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  if px.doneval[px.me] < seq {
    //fmt.Println(px.me, "increased done from", px.doneval[px.me], "to", seq)
  	px.doneval[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.maxinst
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
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  var min int
  elem, ok := px.doneval[0]
  if !ok {
    return 0
  } else {
    min = elem
  }
  for i := 1; i < len(px.peers); i++ {
    elem, ok = px.doneval[i]
    if !ok {
      return 0
    } else {
      if elem < min {
        min = elem
      }
    }
  }
  for x := 0; x <= min; x++ {
    delete(px.instances, x)
  }
  //fmt.Println("min called, min for", px.me, "is", (min+1))
  return (min+1)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  //fmt.Println("checking status", px.me, seq, px.instances[seq].CValue)
  if px.instances[seq].CValue != nil {
    return true, px.instances[seq].CValue
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//handler for acceptor for prepare
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  _, ok := px.instances[args.Seqnum]
  if !ok {
    px.instances[args.Seqnum] = AccVar{-1, -1, nil, nil}
    if args.Seqnum > px.maxinst {
      px.maxinst = args.Seqnum
    }
  }

  if args.Pnum > px.instances[args.Seqnum].highP && px.instances[args.Seqnum].CValue == nil {
    var tmp = px.instances[args.Seqnum]
    tmp.highP = args.Pnum
    px.instances[args.Seqnum] = tmp
    reply.Ok = true
    reply.PRnum = px.instances[args.Seqnum].highA
    reply.PRval = px.instances[args.Seqnum].highV
  } else {
    reply.Ok = false
    reply.CommitedValue = px.instances[args.Seqnum].CValue
  }

  for k := 0; k < len(px.peers); k++ {
    if px.doneval[k] < args.Donemap[k] {
      px.doneval[k] = args.Donemap[k]
    }
    //fmt.Println(px.me, "Map updated", k, "to", px.doneval[k] )
  }
  reply.Donemap = px.doneval
  return nil
}

//handler for acceptor for accept
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  if args.AId >= px.instances[args.Seqnum].highP {
    var tmp = px.instances[args.Seqnum]
    tmp.highP = args.AId
    tmp.highA = args.AId
    tmp.highV = args.Aval
    px.instances[args.Seqnum] = tmp
    reply.Ok = true
    reply.ARnum = args.AId
  } else {
    reply.Ok = false
    reply.ARnum = px.instances[args.Seqnum].highP
  }
  return nil
}

//handler for commit
func (px *Paxos) Commit(args *CommitArgs, reply *CommitReply) error {
  //fmt.Println("commited", px.me, args.Seqnum, args.Value)
  px.mu.Lock()
  tmp := px.instances[args.Seqnum]
  tmp.CValue = args.Value
  px.instances[args.Seqnum] = tmp
  //fmt.Println(px.me, "commited to", args.Seqnum, px.instances[args.Seqnum].CValue)
  px.mu.Unlock()
  return nil
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.maxinst = -1
  px.instances = make(map[int]AccVar)
  px.doneval = make(map[int]int)
  for i := 0; i < len(px.peers); i++ {
    px.doneval[i] = -1
  }

  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
