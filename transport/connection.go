package transport

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/mozgul/gossip/base"
	"github.com/mozgul/gossip/log"
	"github.com/mozgul/gossip/parser"

	"github.com/tevino/abool"
)

type connection struct {
	baseConn       net.Conn
	isStreamed     bool
	parser         parser.Parser
	parsedMessages chan base.SipMessage
	parserErrors   chan error
	output         chan base.SipMessage
	isOpen         AtomicBool
}

func NewConn(baseConn net.Conn, output chan base.SipMessage) *connection {
	var isStreamed bool
	switch baseConn.(type) {
	case *net.UDPConn:
		isStreamed = false
	case *net.TCPConn:
		isStreamed = true
	case *tls.Conn:
		isStreamed = true
	default:
		log.Severe("Conn object %v is not a known connection type. Assume it's a streamed protocol, but this may cause messages to be rejected")
	}
	connection := connection{baseConn: baseConn, isStreamed: isStreamed}

	connection.parsedMessages = make(chan base.SipMessage)
	connection.parserErrors = make(chan error)
	connection.output = output
	connection.parser = parser.NewParser(connection.parsedMessages,
		connection.parserErrors,
		connection.isStreamed)
	connection.isOpen = abool.NewBool(true)

	go connection.read()
	go connection.pipeOutput()

	return &connection
}

func (connection *connection) Send(msg base.SipMessage) (err error) {
	msgData := msg.String()
	log.Debug("Sending message over connection %p: %s (Total %v bytes)", connection, msg.Short(), len(msgData))
	n, err := connection.baseConn.Write([]byte(msgData))

	if err != nil {
		log.Debug("Connection: %p Received error [%v]", connection, err)
		connection.Close()
		return
	}

	if n != len(msgData) {
		return fmt.Errorf("not all data was sent when dispatching '%s' to %s",
			msg.Short(), connection.baseConn.RemoteAddr())
	}

	return
}

func (connection *connection) Close() error {
	log.Debug("Closing connection %p [%#v]", connection, connection)
	connection.isOpen.UnSet()
	connection.parser.Stop()
	log.Debug("Closing connection %p [%#v]", connection, connection)
	return connection.baseConn.Close()
}

func (connection *connection) read() {
	buffer := make([]byte, c_BUFSIZE)
	for {
		log.Debug("Connection %p waiting for new data on sock", connection)
		num, err := connection.baseConn.Read(buffer)
		if err != nil {
			// If connections are broken, just let them drop.
			log.Debug("Lost connection [%p] to %s on %s",
				connection,
				connection.baseConn.RemoteAddr().String(),
				connection.baseConn.LocalAddr().String())
			connection.Close()

			return
		}

		log.Debug("Connection %p received %d bytes", connection, num)
		pkt := append([]byte(nil), buffer[:num]...)
		connection.parser.Write(pkt)
	}
}

func (connection *connection) pipeOutput() {
	for {
		select {
		case message, ok := <-connection.parsedMessages:
			if ok {
				log.Debug("Connection %p from %s to %s received message over the wire: %s",
					connection,
					connection.baseConn.RemoteAddr(),
					connection.baseConn.LocalAddr(),
					message.Short())
				connection.output <- message
			} else {
				break
			}
		case err, ok := <-connection.parserErrors:
			if ok {
				// The parser has hit a terminal error. We need to restart it.
				log.Warn("Failed to parse SIP message: %s", err.Error())
				connection.parser = parser.NewParser(connection.parsedMessages,
					connection.parserErrors, connection.isStreamed)
			} else {
				break
			}
		}
	}

	log.Info("Parser stopped in ConnWrapper %v (local addr %s; remote addr %s); stopping listening",
		connection, connection.baseConn.LocalAddr(), connection.baseConn.RemoteAddr())
}
