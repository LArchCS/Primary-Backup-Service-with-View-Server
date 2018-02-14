package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

// Your RPC definitions here.
// forward client requests from primary to backup
// backup reject a direct client request but accept a forwarded request
// transfer of the complete key/value database from the primary to a new backup
type FwdArgs struct {
	Kv map[string]string
}

type FwdReply struct {
	Err Err
}
