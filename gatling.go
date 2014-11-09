package gatling

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
)

const (
	SUB uint16 = iota
	USUB
	PUB
)

type Callback func([]byte)

type Message struct {
	Topic string
	Data  []byte
}

type Gun struct {
	addr           *net.TCPAddr
	conn           *net.TCPConn
	subscriptions  map[string]Callback
	messages       chan *Message
	socketMu       sync.Mutex
	closedMu       sync.RWMutex
	subscriptionMu sync.RWMutex
	closed         bool
	pool           *sync.Pool
}

func NewGun(addr string) (*Gun, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	g := &Gun{
		addr:           tcpAddr,
		subscriptions:  map[string]Callback{},
		messages:       make(chan *Message, 500000),
		closedMu:       sync.RWMutex{},
		subscriptionMu: sync.RWMutex{},
		socketMu:       sync.Mutex{},
		pool:           &sync.Pool{New: func() interface{} { return &Message{} }},
	}

	return g, nil
}

func (g *Gun) Dial() error {
	conn, err := net.DialTCP("tcp", nil, g.addr)
	if err != nil {
		return err
	}
	g.conn = conn
	go g.recv()
	go g.subscriptionDispatcher()
	return nil
}

func (g *Gun) Close() error {
	g.closedMu.Lock()
	g.closed = true
	g.closedMu.Unlock()
	return g.conn.Close()
}

func (g *Gun) Subscribe(topic string, callback Callback) error {
	g.subscriptionMu.RLock()
	if _, ok := g.subscriptions[topic]; ok {
		g.subscriptionMu.RUnlock()
		return fmt.Errorf("Already subscribed to %s", topic)
	}
	g.subscriptionMu.RUnlock()

	var buf bytes.Buffer
	var err error

	g.socketMu.Lock()
	if e := binary.Write(&buf, binary.BigEndian, SUB); e != nil {
		err = e
	}
	topicSize := uint32(len(topic))
	if e := binary.Write(&buf, binary.BigEndian, topicSize); e != nil {
		err = e
	}
	if _, e := buf.WriteString(topic); e != nil {
		err = e
	}
	if _, e := g.conn.Write(buf.Bytes()); e != nil {
		err = e
	}
	g.socketMu.Unlock()

	if err == nil {
		g.subscriptionMu.Lock()
		g.subscriptions[topic] = callback
		g.subscriptionMu.Unlock()
	}

	return err
}

func (g *Gun) Unsubscribe(topic string) error {
	g.subscriptionMu.RLock()
	if _, ok := g.subscriptions[topic]; !ok {
		g.subscriptionMu.RUnlock()
		return fmt.Errorf("Not subscribed to %s", topic)
	}
	g.subscriptionMu.RUnlock()

	size := uint32(len(topic))
	var buf bytes.Buffer
	var err error

	g.socketMu.Lock()
	if e := binary.Write(&buf, binary.BigEndian, USUB); e != nil {
		err = e
	}
	if e := binary.Write(&buf, binary.BigEndian, size); e != nil {
		err = e
	}
	if _, e := buf.WriteString(topic); e != nil {
		err = e
	}
	if _, e := g.conn.Write(buf.Bytes()); e != nil {
		err = e
	}
	g.socketMu.Unlock()
	return err
}

func (g *Gun) Publish(topic string, msg []byte) error {
	var size uint32
	size = uint32(len(topic)) + uint32(len(msg)) + 4
	topicSize := uint32(len(topic))
	var err error
	var buf bytes.Buffer

	g.socketMu.Lock()
	if e := binary.Write(&buf, binary.BigEndian, PUB); e != nil {
		err = e
	}
	if e := binary.Write(&buf, binary.BigEndian, size); e != nil {
		err = e
	}
	if e := binary.Write(&buf, binary.BigEndian, topicSize); e != nil {
		err = e
	}
	if _, e := buf.WriteString(topic); e != nil {
		err = e
	}
	if _, e := buf.Write(msg); e != nil {
		err = e
	}
	if _, e := g.conn.Write(buf.Bytes()); e != nil {
		err = e
	}
	g.socketMu.Unlock()
	return err
}

func (g *Gun) recv() {
	for {
		g.closedMu.RLock()
		if g.closed {
			g.closedMu.RUnlock()
			break
		}
		g.closedMu.RUnlock()

		var proto uint16
		if err := binary.Read(g.conn, binary.BigEndian, &proto); err != nil {
			log.Println(err)
			continue
		}

		if proto != PUB {
			log.Println("Invalid protocol frame")
			continue
		}

		var size uint32
		if err := binary.Read(g.conn, binary.BigEndian, &size); err != nil {
			log.Println("Invalid protocol frame")
			continue
		}

		var topicSize uint32
		if err := binary.Read(g.conn, binary.BigEndian, &topicSize); err != nil {
			log.Println("Invalid protocol frame")
			continue
		}

		topic := make([]byte, topicSize)
		if read, err := g.conn.Read(topic); err != nil || uint32(read) != topicSize {
			log.Println("Invalid protocol frame")
			continue
		}

		bodySize := size - topicSize - 4
		body := make([]byte, bodySize)
		if read, err := g.conn.Read(body); err != nil || uint32(read) != bodySize {
			log.Println("Invalid protocol frame")
			continue
		}

		msg := g.pool.Get().(*Message)
		msg.Topic = string(topic)
		msg.Data = body

		select {
		case g.messages <- msg:
			break
		default:
			// Backpressure, drop the message.
			g.pool.Put(msg)
		}
	}
}

func (g *Gun) subscriptionDispatcher() {
	for {
		g.closedMu.RLock()
		if g.closed {
			g.closedMu.RUnlock()
			break
		}
		g.closedMu.RUnlock()

		msg := <-g.messages

		g.subscriptionMu.RLock()
		if callback, ok := g.subscriptions[msg.Topic]; ok {
			go func(msg *Message) {
				data := make([]byte, len(msg.Data))
				copy(data, msg.Data)
				callback(data)
				g.pool.Put(msg)
			}(msg)
		} else {
			g.pool.Put(msg)
		}
		g.subscriptionMu.RUnlock()
	}
}
