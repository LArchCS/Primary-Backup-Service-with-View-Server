package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"
import "errors"

/*
For network failures between Primary and Backup
(FwdPut failure, and FwdState failure),
there are two options available for each failure:
1. assumes backup is bad, thus remove
2. sleep for a short period and try again
3. (or stack at P and send to B next Ping for FwdPut)
Here my choice is an easier combination in my opinion -
- if FwdPut fails, keep trying (to avoid complete state transfer next time)
- if FwdState fails, be aware backup is bad, and try at next Ping
*/

type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk

  // Your declarations here.
  view       viewservice.View  // track the view on server
	kv         map[string]string // track of key/value state
	am_primary bool              // am I a primary?
	backup     string            // track of primary's Backup
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// non-primary server should reject direct contacts
	if !pb.am_primary {
		reply.Value = ""
		reply.Err = ErrWrongServer
		return errors.New(pb.me + " is not Primary")
	}
	value, ok := pb.kv[args.Key]
	reply.Value = value
	if !ok {
		reply.Err = ErrNoKey
	}
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	reply.Err = OK
	// Your code here.
	// non-primary server should reject direct contacts
	if !pb.am_primary {
		reply.Err = ErrWrongServer
		return errors.New(pb.me + " is not Primary")
	}
	pb.kv[args.Key] = args.Value
	if pb.backup == pb.view.Backup && pb.backup != "" { // 2 options here
		// forward Put to the backup server
		for !call(pb.backup, "PBServer.FwdPut", &args, &reply) {
			time.Sleep(viewservice.PingInterval) // if rpc fails, try again
		}
		/*if !call(pb.backup, "PBServer.FwdPut", &args, &reply) { // if fail, assume backup fails
			pb.backup = ""
		}*/
	}
	return nil
}

// function to forward a put() from primary to backup
func (pb *PBServer) FwdPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// non-backup server should reject forwards
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return errors.New(pb.me + " is not Backup")
	}
	pb.kv[args.Key] = args.Value
	reply.Err = OK
	return nil
}

// function to forward complete key/value state from primary to backup
func (pb *PBServer) FwdState(args *FwdArgs, reply *FwdReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// non-backup server should reject forwards
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return errors.New(pb.me + " is not Backup")
	}
	pb.kv = args.Kv
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	view, ok := pb.vs.Ping(pb.view.Viewnum)
	pb.am_primary = (ok == nil && pb.me == view.Primary)
	pb.view = view  // update view at each Ping
	if !pb.am_primary {
		return
	}
	// if backup server has changed in view service,
	// primary server should forward complete key/value database to new backup
	if pb.backup != view.Backup && view.Backup != "" { // 2 options here
		pb.backup = ""
		if call(pb.view.Backup, "PBServer.FwdState", &FwdArgs{pb.kv}, &FwdReply{}) {
			pb.backup = view.Backup
		}
		/*for !call(pb.view.Backup, "PBServer.FwdState", &FwdArgs{pb.kv}, &FwdReply{}) {
			time.Sleep(viewservice.PingInterval) // if rpc fails, try again
		}
		pb.backup = view.Backup // update primary's backup after forward success*/
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  // Your pb.* initializations here.
  pb.view = viewservice.View{}
  pb.kv = make(map[string]string)
  
	rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
