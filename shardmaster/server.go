package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "../paxos"
import "time"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  version int

  configs []Config // indexed by config num
}


type Op struct {
  Action string
  Gid int64
  Qnum int
  MoveShard int
  Server []string
  Id int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  cont := true
  var decidednum int
  for cont {
    numtouse := sm.px.Max() + 1

    tempid := rand.Int63()
    opvalues := Op{"Join", args.GID, 0, 0, args.Servers, tempid}
    sm.px.Start(numtouse, opvalues)
    wtime := 3*time.Millisecond
    time.Sleep(2*time.Millisecond)
    for {
      done, value := sm.px.Status(numtouse)
      if done {
        val := value.(Op)
        if val.Id == tempid {
          decidednum = numtouse
          cont = false
        }
        break
      } else {
        time.Sleep(wtime)
        if wtime < 50 * time.Millisecond {
          wtime = 2 * wtime
        }
      }
    }
  }

  sm.mu.Lock()
  sm.UpdateConfigs(decidednum)
  sm.mu.Unlock()

  return nil
}

func (sm *ShardMaster) Joinfunc(ngid int64, serv []string) {
  lastconfig := sm.configs[len(sm.configs)-1]
  numtomove := NShards/(len(lastconfig.Groups)+1)
  //fmt.Println("original: ", lastconfig.Shards)

  var mshards [NShards]int64
  for i, v := range lastconfig.Shards {
    mshards[i] = v
  }
  isrepeat := false
  //creating a counter for number of shards each group has
  temp := make(map[int64]int)
  for key, _ := range lastconfig.Groups {
    if key == ngid {
      isrepeat = true
    }
    temp[key] = 0
  }
  //counting the number of shards for each group
  for _, val := range mshards {
    temp[val]++
  }

  if !isrepeat {
    //redistributing
    for i := 0; i < numtomove; i++ {
      //fmt.Println("i:", i)
      //fmt.Println(len(temp))
      totake := max(temp)
      //fmt.Println(temp)
      temp[totake]--
      for index, value := range mshards {
        if value == totake {
          mshards[index] = ngid
          break
        }
      }
      //fmt.Println(mshards)
    }
  }

  //creating the new groups map
  ngroups := make(map[int64][]string)
  for key, val := range lastconfig.Groups {
    ngroups[key] = val
  }
  ngroups[ngid] = serv

  nconfig := Config{lastconfig.Num+1, mshards, ngroups}
  sm.configs = append(sm.configs, nconfig)
}

func max(input map[int64]int) int64 {
  var id int64 = 0
  var max int = -1
  for key, value := range input {
    if value > max {
      id = key
      max = value
    } else if value == max {
      if key < id {
        id = key
      }
    }
  }
  return id
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  cont := true
  var decidednum int
  for cont {
    numtouse := sm.px.Max() + 1

    tempid := rand.Int63()
    opvalues := Op{"Leave", args.GID, 0, 0, nil, tempid}
    sm.px.Start(numtouse, opvalues)
    wtime := 3*time.Millisecond
    time.Sleep(2*time.Millisecond)
    for {
      done, value := sm.px.Status(numtouse)
      if done {
        val := value.(Op)
        if val.Id == tempid {
          decidednum = numtouse
          cont = false
        }
        break
      } else {
        time.Sleep(wtime)
        if wtime < 50 * time.Millisecond {
          wtime = 2 * wtime
        }
      }
    }
  }

  sm.mu.Lock()
  sm.UpdateConfigs(decidednum)
  sm.mu.Unlock()

  return nil
}

func (sm *ShardMaster) Leavefunc(rgid int64) {
  lastconfig := sm.configs[len(sm.configs)-1]

  var mshards [NShards]int64
  for i, v := range lastconfig.Shards {
    mshards[i] = v
  }

  //creating a counter for number of shards each group has
  temp := make(map[int64]int)
  for key, _ := range lastconfig.Groups {
    if key != rgid {
      temp[key] = 0
    }
  }
  //counting the number of shards for each group
  for _, val := range mshards {
    if val != rgid {
      temp[val]++
    }
  }
  //redistributing
  for index, gid := range mshards {
    if gid == rgid {
      togive := min(temp)
      temp[togive]++
      mshards[index] = togive
    }
  }

  //creating the new groups map
  ngroups := make(map[int64][]string)
  for key, val := range lastconfig.Groups {
    if key != rgid {
      ngroups[key] = val
    }
  }

  nconfig := Config{lastconfig.Num+1, mshards, ngroups}
  sm.configs = append(sm.configs, nconfig)
}

func min(input map[int64]int) int64 {
  var id int64 = 0
  var min int = NShards + 1
  for key, value := range input {
    if value < min {
      id = key
      min = value
    } else if value == min {
      if key < id {
        id = key
      }
    }
  }
  return id
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  cont := true
  var decidednum int
  for cont {
    numtouse := sm.px.Max() + 1
    tempid := rand.Int63()
    opvalues := Op{"Move", args.GID, 0, args.Shard, nil, tempid}

    sm.px.Start(numtouse, opvalues)

    wtime := 3*time.Millisecond
    time.Sleep(2*time.Millisecond)
    for {
      done, value := sm.px.Status(numtouse)
      if done {
        val := value.(Op)
        if val.Id == tempid {
          decidednum = numtouse
          cont = false
        }
        break
      } else {
        time.Sleep(wtime)
        if wtime < 50 * time.Millisecond {
          wtime = 2 * wtime
        }
      }
    }
  }

  sm.mu.Lock()
  sm.UpdateConfigs(decidednum)
  sm.mu.Unlock()

  return nil
}

func (sm *ShardMaster) Movefunc(Shard int, gid int64) {
  lastconfig := sm.configs[len(sm.configs)-1]

  var mshards [NShards]int64
  for i, v := range lastconfig.Shards {
    mshards[i] = v
  }
  mshards[Shard] = gid

  ngroups := make(map[int64][]string)
  for key, val := range lastconfig.Groups {
    ngroups[key] = val
  }

  nconfig := Config{lastconfig.Num+1, mshards, ngroups}
  sm.configs = append(sm.configs, nconfig)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  cont := true
  var decidednum int
  for cont {
    numtouse := sm.px.Max() + 1

    tempid := rand.Int63()
    opvalues := Op{"Query", 0, args.Num, 0, nil, tempid}
    sm.px.Start(numtouse, opvalues)
    wtime := 3*time.Millisecond
    for {
      done, value := sm.px.Status(numtouse)
      if done {
        val := value.(Op)
        if val.Id == tempid {
          decidednum = numtouse
          cont = false
        }
        break
      } else {
        time.Sleep(wtime)
        if wtime < 50 * time.Millisecond {
          wtime = 2 * wtime
        }
      }
    }
  }

  //updating configs
  sm.mu.Lock()
  sm.UpdateConfigs(decidednum)
  //done updating configs here

  if args.Num == -1 || args.Num >= len(sm.configs) {
    reply.Config = sm.configs[len(sm.configs)-1]
  } else {
    reply.Config = sm.configs[args.Num]
  }
  sm.mu.Unlock()
  return nil
}

//must hold lock when calling this function
func (sm *ShardMaster) UpdateConfigs(decidednum int) {
  for i := sm.version; i < decidednum; i ++ {
    incont := true
    for incont {
      decided, value := sm.px.Status(i)
      if decided {
        incont = false
        val := value.(Op)
        if val.Action == "Join" {
          sm.Joinfunc(val.Gid, val.Server)
        } else if val.Action == "Leave" {
          sm.Leavefunc(val.Gid)
        } else if val.Action == "Move" {
          sm.Movefunc(val.MoveShard, val.Gid)
        } else if val.Action == "Query" {
          //do nothing
        } else {
          fmt.Println("ERR: no matched function in update of query")
        }
      } else {
        //fmt.Println("waiting to update again")
        time.Sleep(3*time.Millisecond)
      }
    }
  }
  sm.version = decidednum
  sm.px.Done(sm.version-1)
  //fmt.Println("called done: ", sm.me, decidednum -1)
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me
  sm.version = 0

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
