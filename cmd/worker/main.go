package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var (
	listen = flag.String("worker", "/tmp/worker.sock", "path to listen UDS")
)

func main() {
	tmpName := *listen + strconv.Itoa(os.Getpid())

	listener, err := Listen(tmpName)
	if err != nil {
		panic(err)
	}

	err = os.Rename(tmpName, *listen)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go handle(conn)
	}
}

type unixgramListener struct {
	conn *net.UnixConn
	raw  syscall.RawConn
}

func Listen(listen string) (*unixgramListener, error) {
	addr, err := net.ResolveUnixAddr("unixgram", listen)
	if err != nil {
		return nil, err
	}

	os.Remove(listen) // error ignored in pourpose

	conn, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		return nil, err
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &unixgramListener{conn: conn, raw: raw}, nil
}

func (s *unixgramListener) Accept() (net.Conn, error) {
	fd, err := accept(s.raw)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(fd), "")
	defer f.Close()

	return net.FileConn(f)
}

func accept(conn syscall.RawConn) (afd int, err error) {
	rerr := conn.Read(
		func(ufd uintptr) bool {
			return readFD(int(ufd), &afd, &err)
		},
	)
	if rerr != nil {
		err = rerr
	}

	return
}

func readFD(ufd int, afd *int, err *error) bool {
	var msg []byte
	msg, *err = readDatagram(ufd)
	if *err != nil {
		return *err != syscall.EAGAIN
	}

	*afd, *err = parseDatagram(msg)

	return true
}

var (
	ErrInvalidSize    = errors.New("invalid size")
	ErrInvalidMessage = errors.New("invalid message")
)

func readDatagram(ufd int) ([]byte, error) {
	var (
		msgSize = unix.CmsgSpace(4)
		name    = make([]byte, 4096)
		msg     = make([]byte, msgSize)
	)

	n, msgGot, _, _, err := unix.Recvmsg(ufd, name, msg, 0)
	if err != nil {
		return nil, err
	}

	if n >= 4096 || msgGot != msgSize {
		return nil, ErrInvalidSize
	}

	return msg, nil
}

func parseDatagram(data []byte) (int, error) {
	msgs, err := unix.ParseSocketControlMessage(data)
	if err != nil {
		return 0, err
	}

	if len(msgs) != 1 {
		return 0, ErrInvalidMessage
	}

	fds, err := unix.ParseUnixRights(&msgs[0])
	if err != nil {
		return 0, err
	}

	if len(fds) < 1 {
		return 0, ErrInvalidMessage
	}

	return fds[0], nil
}

func (s *unixgramListener) Close() error {
	return s.conn.Close()
}

func (s *unixgramListener) Addr() net.Addr {
	return s.conn.LocalAddr()
}

func handle(conn net.Conn) {
	defer conn.Close()

	log.Printf("connection from %q", conn.RemoteAddr().String())

	t := time.NewTicker(time.Second)
	for curTime := range t.C {
		_, err := fmt.Fprintf(conn, "%s %d\n", curTime.Format(time.RFC3339), os.Getpid())
		if err != nil {
			log.Printf("error sending to %s: %v", conn.RemoteAddr().String(), err)
			return
		}
	}
}
