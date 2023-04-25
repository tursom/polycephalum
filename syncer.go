package polycephalum

import (
	"time"

	"github.com/tursom/GoCollections/lang"

	"github.com/tursom/polycephalum/proto/m"
)

func (p *impl[M]) startSyncer(wc lang.SendChannel[*m.Msg]) {
	if wc == nil {
		return
	}

	go p.startNodeStateSyncer(wc)
	go p.startBroadcastSyncer(wc)
}

func (p *impl[M]) startNodeStateSyncer(wc lang.SendChannel[*m.Msg]) {
	ticker := time.NewTicker(time.Second)
	i := 0

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

		i++
		if i > 120 {
			ticker.Reset(time.Minute * 5)
		} else if i > 60 {
			ticker.Reset(time.Minute)
		} else if i > 15 {
			ticker.Reset(time.Second * 15)
		}
		<-ticker.C
	}
}

func (p *impl[M]) startBroadcastSyncer(wc lang.SendChannel[*m.Msg]) {
	ticker := time.NewTicker(time.Second)
	i := 0

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

		i++
		if i > 120 {
			ticker.Reset(time.Minute * 5)
		} else if i > 60 {
			ticker.Reset(time.Minute)
		} else if i > 15 {
			ticker.Reset(time.Second * 15)
		}
		<-ticker.C
	}
}
