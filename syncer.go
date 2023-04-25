package polycephalum

import (
	"github.com/tursom/GoCollections/lang"

	"github.com/tursom/polycephalum/proto/m"
)

func (p *impl[M]) startSyncer(wc lang.SendChannel[*m.Msg]) {
	if wc == nil {
		return
	}

	// TODO impl
	go p.startNodeStateSyncer(wc)
}

func (p *impl[M]) startNodeStateSyncer(wc lang.SendChannel[*m.Msg]) {
	for {
		if !trySend(wc, m.Build(func(msg *m.Msg) {
			msg.BuildSyncNodeStateRequest(func(r *m.SyncNodeStateRequest) {
				r.States = append(r.States, &m.NodeState{
					Id:    p.id,
					State: p.state,
					Jump:  0,
				})

				for _, node := range p.net.Snap() {
					r.States = append(r.States, &m.NodeState{
						Id:    node.Id,
						State: node.State,
						Jump:  node.Jmp + 1,
					})

				}
			})
		})) {
			return
		}
	}
}

func (p *impl[M]) startBroadcastSyncer(wc lang.SendChannel[*m.Msg]) {
	for {
		if !trySend(wc, m.Build(func(msg *m.Msg) {
			msg.BuildSyncBroadcastMsg(func(r *m.SyncBroadcastMsg) {
				for _, id := range p.broadcast.Snap() {
					version, h := p.broadcast.NodeHash(id)

					r.Snapshots = append(r.Snapshots, &m.SyncBroadcastMsg_Snapshot{
						Node:    id,
						Version: version,
						Hash:    h,
					})
				}
			})
		})) {
			return
		}
	}
}
