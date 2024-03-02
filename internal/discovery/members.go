package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

const RpcAddrTagKey = "rpc_addr"

type Membership struct {
	cfg     *Config
	handler Handler

	serf   *serf.Serf
	events chan serf.Event
	logger *zap.Logger
}

func New(handler Handler, cfg *Config) (*Membership, error) {
	m := &Membership{
		cfg:     cfg,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := m.setupSerf(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.cfg.BindAddr)
	if err != nil {
		return err
	}

	m.events = make(chan serf.Event)

	cfg := serf.DefaultConfig()
	cfg.Init()
	cfg.MemberlistConfig.BindAddr = addr.IP.String()
	cfg.MemberlistConfig.BindPort = addr.Port
	cfg.EventCh = m.events
	cfg.Tags = m.cfg.Tags
	cfg.NodeName = m.cfg.NodeName

	m.serf, err = serf.Create(cfg)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.cfg.StartJoinAddrs != nil {
		_, err := m.serf.Join(m.cfg.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}

				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags[RpcAddrTagKey]); err != nil {
		m.logError(err, "failed to join member", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave member", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String(RpcAddrTagKey, member.Tags[RpcAddrTagKey]),
	)
}
