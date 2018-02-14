package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	dic           map[string]time.Time // map server to their most recent Ping times
	view          View                 // keep track of current view
	idle          []string             // a queue of idle servers
	ack           bool                 // if primary acknowledges when Ping
	primaryActive bool                 // if primary is active
	backupReady   bool                 // if current backup is fully initialized
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	vs.dic[args.Me] = time.Now() // register server ping time
	if vs.view.Viewnum == 0 {    // the first ping
		vs.view.Viewnum = 1
		vs.view.Primary = args.Me
	} else if vs.view.Primary == args.Me {
		if args.Viewnum == 0 { // if a primary restarts, treat as dead
			vs.primaryActive = false
		} else { // else, update primary acknowledges
			vs.ack = (vs.view.Viewnum == args.Viewnum)
		}
	} else if vs.view.Backup == args.Me { // if is backup, update backup ready state
		vs.backupReady = (args.Viewnum == vs.view.Viewnum)
	} else if args.Me != vs.view.Primary && args.Me != vs.view.Backup { // idle
		vs.idle = enQueue(vs.idle, args.Me)
	}
	reply.View = vs.view
	return nil
}

// add an idle server into queue
func enQueue(queue []string, entry string) []string {
	for i := 0; i < len(queue); i++ {
		if queue[i] == entry {
			return queue
		}
	}
	return append(queue, entry)
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	if vs.view.Primary != "" {
		if time.Now().Sub(vs.dic[vs.view.Primary]) > (DeadPings * PingInterval) {
			vs.primaryActive = false
		}
	}
	if vs.view.Backup != "" {
		if time.Now().Sub(vs.dic[vs.view.Backup]) > (DeadPings * PingInterval) {
			vs.view.Backup = ""
			vs.backupReady = false
		}
	}
	vs.advanceView()
}

func (vs *ViewServer) advanceView() {
	if !vs.ack {
		return
	}
	curt := vs.view.Viewnum
	// promote new primary from backup
	if vs.primaryActive == false && vs.view.Backup != "" && vs.backupReady {
		vs.view.Primary = vs.view.Backup
		vs.primaryActive = true
		vs.view.Backup = ""
		vs.backupReady = false
		vs.view.Viewnum = curt + 1
	}
	// promote new backup from idle
	if vs.view.Backup == "" && len(vs.idle) > 0 {
		vs.view.Backup = vs.idle[0]
		vs.idle = vs.idle[1:]
		vs.view.Viewnum = curt + 1
	}
	vs.ack = (vs.view.Viewnum == curt)
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{}
	vs.dic = make(map[string]time.Time)
	vs.idle = make([]string, 0)
	vs.primaryActive = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
