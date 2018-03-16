package transport

import (
	"github.com/mozgul/gossip/base"
	"github.com/mozgul/gossip/log"
	"github.com/mozgul/gossip/parser"
)

import (
	"net"
)

type Udp struct {
	connTable
	listeningPoints []*net.UDPConn
	output          chan base.SipMessage
	stop            bool
}

func NewUdp(output chan base.SipMessage) (*Udp, error) {
	newUdp := Udp{listeningPoints: make([]*net.UDPConn, 0), output: output}
	newUdp.connTable.Init()
	return &newUdp, nil
}

func (udp *Udp) Listen(address string) error {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	lp, err := net.ListenUDP("udp", addr)

	if err == nil {
		udp.listeningPoints = append(udp.listeningPoints, lp)
		go udp.listen(lp)
	}

	return err
}

func (udp *Udp) IsStreamed() bool {
	return false
}

func (udp *Udp) getConnection(addr string) (*connection, error) {
	conn := udp.connTable.GetConn(addr)
	if conn == nil {
		log.Debug("No stored connection for address %s; generate a new one", addr)
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}

		baseConn, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			return nil, err
		}
		conn = NewConn(baseConn, udp.output)
	} else {
		conn = udp.connTable.GetConn(addr)
	}

	udp.connTable.Notify(addr, conn)
	return conn, nil
}

func (udp *Udp) Send(addr string, msg base.SipMessage) error {
	log.Debug("Sending message %s to %s", msg.Short(), addr)

	conn, err := udp.getConnection(addr)
	if err != nil {
		return err
	}

	err = conn.Send(msg)

	return err
}

func (udp *Udp) listen(conn *net.UDPConn) {
	log.Info("Begin listening for UDP on address %s", conn.LocalAddr())

	buffer := make([]byte, c_BUFSIZE)
	for {
		num, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if udp.stop {
				log.Info("Stopped listening for UDP on %s", conn.LocalAddr)
				break
			} else {
				log.Severe("Failed to read from UDP buffer: " + err.Error())
				continue
			}
		}

		pkt := append([]byte(nil), buffer[:num]...)
		go func() {
			msg, err := parser.ParseMessage(pkt)
			if err != nil {
				log.Warn("Failed to parse SIP message: %s", err.Error())
			} else {
				udp.output <- msg
			}
		}()
	}
}

func (udp *Udp) Stop() {
	udp.connTable.Stop()
	udp.stop = true
	for _, lp := range udp.listeningPoints {
		lp.Close()
	}
}
