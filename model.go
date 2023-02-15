package main

import (
	"time"
)

type EventType int64

const (
	// from https://github.com/libp2p/go-libp2p-pubsub/blob/master/pb/trace.pb.go
	// and https://github.com/filecoin-project/lotus/blob/master/node/modules/tracer/tracer.go

	EventTypePublishMessage   EventType = 0
	EventTypeRejectMessage    EventType = 1
	EventTypeDuplicateMessage EventType = 2
	EventTypeDeliverMessage   EventType = 3
	EventTypeAddPeer          EventType = 4
	EventTypeRemovePeer       EventType = 5
	EventTypeRecvRPC          EventType = 6
	EventTypeSendRPC          EventType = 7
	EventTypeDropRPC          EventType = 8
	EventTypeJoin             EventType = 9
	EventTypeLeave            EventType = 10
	EventTypeGraft            EventType = 11
	EventTypePrune            EventType = 12
	EventTypePeerScore        EventType = 100
)

func (e EventType) Key() string {
	switch e {
	case EventTypePublishMessage:
		return "publish_message"
	case EventTypeRejectMessage:
		return "reject_message"
	case EventTypeDuplicateMessage:
		return "duplicate_message"
	case EventTypeDeliverMessage:
		return "deliver_message"
	case EventTypeAddPeer:
		return "add_peer"
	case EventTypeRemovePeer:
		return "remove_peer"
	case EventTypeRecvRPC:
		return "recv_rpv"
	case EventTypeSendRPC:
		return "send_rpc"
	case EventTypeDropRPC:
		return "drop_rpc"
	case EventTypeJoin:
		return "join"
	case EventTypeLeave:
		return "leave"
	case EventTypeGraft:
		return "graft"
	case EventTypePrune:
		return "prune"
	case EventTypePeerScore:
		return "peer_score"
	default:
		return "unknown_event_type"
	}
}

type TraceEvent struct {
	Type             *EventType             `json:"type,omitempty"`
	PeerID           []byte                 `json:"peerID,omitempty"`
	Timestamp        *int64                 `json:"timestamp,omitempty"` // nanoseconds
	PublishMessage   *PublishMessageEvent   `json:"publishMessage,omitempty"`
	RejectMessage    *RejectMessageEvent    `json:"rejectMessage,omitempty"`
	DuplicateMessage *DuplicateMessageEvent `json:"duplicateMessage,omitempty"`
	DeliverMessage   *DeliverMessageEvent   `json:"deliverMessage,omitempty"`
	AddPeer          *AddPeerEvent          `json:"addPeer,omitempty"`
	RemovePeer       *RemovePeerEvent       `json:"removePeer,omitempty"`
	RecvRPC          *RecvRPCEvent          `json:"recvRPC,omitempty"`
	SendRPC          *SendRPCEvent          `json:"sendRPC,omitempty"`
	DropRPC          *DropRPCEvent          `json:"dropRPC,omitempty"`
	Join             *JoinEvent             `json:"join,omitempty"`
	Leave            *LeaveEvent            `json:"leave,omitempty"`
	Graft            *GraftEvent            `json:"graft,omitempty"`
	Prune            *PruneEvent            `json:"prune,omitempty"`
	PeerScore        *PeerScoreEvent        `json:"peerScore,omitempty"`
	SourceAuth       *string                `json:"sourceAuth,omitempty"`
}

type PublishMessageEvent struct {
	MessageID []byte  `json:"messageID,omitempty"`
	Topic     *string `json:"topic,omitempty"`
}

type RejectMessageEvent struct {
	MessageID    []byte  `json:"messageID,omitempty"`
	ReceivedFrom []byte  `json:"receivedFrom,omitempty"`
	Reason       *string `json:"reason,omitempty"`
	Topic        *string `json:"topic,omitempty"`
}

type DuplicateMessageEvent struct {
	MessageID    []byte  `json:"messageID,omitempty"`
	ReceivedFrom []byte  `json:"receivedFrom,omitempty"`
	Topic        *string `json:"topic,omitempty"`
}

type DeliverMessageEvent struct {
	MessageID    []byte  `json:"messageID,omitempty"`
	Topic        *string `json:"topic,omitempty"`
	ReceivedFrom []byte  `json:"receivedFrom,omitempty"`
}

type AddPeerEvent struct {
	PeerID []byte  `json:"peerID,omitempty"`
	Proto  *string `json:"proto,omitempty"`
}

type RemovePeerEvent struct {
	PeerID []byte `json:"peerID,omitempty"`
}

type RecvRPCEvent struct {
	ReceivedFrom []byte        `json:"receivedFrom,omitempty"`
	Meta         *RPCMetaEvent `json:"meta,omitempty"`
}

type SendRPCEvent struct {
	SendTo []byte        `json:"sendTo,omitempty"`
	Meta   *RPCMetaEvent `json:"meta,omitempty"`
}

type DropRPCEvent struct {
	SendTo []byte        `json:"sendTo,omitempty"`
	Meta   *RPCMetaEvent `json:"meta,omitempty"`
}

type JoinEvent struct {
	Topic *string `json:"topic,omitempty"`
}

type LeaveEvent struct {
	Topic *string `json:"topic,omitempty"`
}

type GraftEvent struct {
	PeerID []byte  `json:"peerID,omitempty"`
	Topic  *string `json:"topic,omitempty"`
}

type PruneEvent struct {
	PeerID []byte  `json:"peerID,omitempty"`
	Topic  *string `json:"topic,omitempty"`
}

type RPCMetaEvent struct {
	Messages     []*MessageMetaEvent `json:"messages,omitempty"`
	Subscription []*SubMetaEvent     `json:"subscription,omitempty"`
	Control      *ControlMetaEvent   `json:"control,omitempty"`
}

type MessageMetaEvent struct {
	MessageID []byte  `json:"messageID,omitempty"`
	Topic     *string `json:"topic,omitempty"`
}

type SubMetaEvent struct {
	Subscribe *bool   `json:"subscribe,omitempty"`
	Topic     *string `json:"topic,omitempty"`
}

type ControlMetaEvent struct {
	Ihave []*ControlIHaveMetaEvent `json:"ihave,omitempty"`
	Iwant []*ControlIWantMetaEvent `json:"iwant,omitempty"`
	Graft []*ControlGraftMetaEvent `json:"graft,omitempty"`
	Prune []*ControlPruneMetaEvent `json:"prune,omitempty"`
}

type ControlIHaveMetaEvent struct {
	Topic      *string  `json:"topic,omitempty"`
	MessageIDs [][]byte `json:"messageIDs,omitempty"`
}

type ControlIWantMetaEvent struct {
	MessageIDs [][]byte `json:"messageIDs,omitempty"`
}

type ControlGraftMetaEvent struct {
	Topic *string `json:"topic,omitempty"`
}

type ControlPruneMetaEvent struct {
	Topic *string  `json:"topic,omitempty"`
	Peers [][]byte `json:"peers,omitempty"`
}

type TopicScoreEvent struct {
	Topic                    string        `json:"topic"`
	TimeInMesh               time.Duration `json:"timeInMesh"`
	FirstMessageDeliveries   float64       `json:"firstMessageDeliveries"`
	MeshMessageDeliveries    float64       `json:"meshMessageDeliveries"`
	InvalidMessageDeliveries float64       `json:"invalidMessageDeliveries"`
}

type PeerScoreEvent struct {
	PeerID             []byte            `json:"peerID"`
	Score              float64           `json:"score"`
	AppSpecificScore   float64           `json:"appSpecificScore"`
	IPColocationFactor float64           `json:"ipColocationFactor"`
	BehaviourPenalty   float64           `json:"behaviourPenalty"`
	Topics             []TopicScoreEvent `json:"topics"`
}
