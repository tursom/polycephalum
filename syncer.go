package polycephalum

import (
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/time"

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
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()

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
						Jump:  node.Jmp,
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
		} else if i > 5 {
			ticker.Reset(time.Second * 15)
		}
		if _, ok := <-ticker.C; !ok {
			return
		}
	}
}

func (p *impl[M]) startBroadcastSyncer(wc lang.SendChannel[*m.Msg]) {
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()

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
		} else if i > 5 {
			ticker.Reset(time.Second * 15)
		}
		if _, ok := <-ticker.C; !ok {
			return
		}
	}
}
