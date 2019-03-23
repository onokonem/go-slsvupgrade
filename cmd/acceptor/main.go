package main

import (
	"flag"
	"log"
	"net"

	"golang.org/x/sys/unix"
)

var (
	listen = flag.String("listen", ":8080", "address to listen")
	worker = flag.String("worker", "/tmp/worker.sock", "path to worker UDS")
)

func main() {
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		err = passTCPConn(conn.(*net.TCPConn), *worker)
		if err != nil {
			log.Printf("Error passing connection: %v: %v", conn, err)
		}

		log.Printf("Connection passed: %q", conn.RemoteAddr().String())
	}
}

func passTCPConn(conn *net.TCPConn, worker string) error {
	defer conn.Close()

	f, err := conn.File()
	if err != nil {
		return err
	}
	defer f.Close()

	return passFD(int(f.Fd()), worker)
}

func passFD(fd int, worker string) error {

	uds, err := net.Dial("unixgram", worker)
	if err != nil {
		return err
	}
	defer uds.Close()

	return passFDRaw(fd, uds.(*net.UnixConn))
}

func passFDRaw(fd int, uds *net.UnixConn) error {
	err := unix.SetNonblock(fd, true)
	if err != nil {
		return err
	}

	scConn, err := uds.SyscallConn()
	if err != nil {
		return err
	}

	return scConn.Write(
		func(ufd uintptr) bool {
			unix.Sendmsg(int(ufd), []byte{'p', 's', 'w', 0}, unix.UnixRights(fd), nil, 0)
			return true
		},
	)
}
