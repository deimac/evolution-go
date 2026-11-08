package whatsmeow_service

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func webMsg(id string, ts uint64) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{ID: proto.String(id)},
		MessageTimestamp: proto.Uint64(ts),
	}}
}

func fakeParse(chat types.JID, m *waWeb.WebMessageInfo) (*events.Message, error) {
	if m.GetKey().GetID() == "bad" {
		return nil, errors.New("bad")
	}
	return &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat},
		ID:            m.GetKey().GetID(),
		Timestamp:     time.Unix(int64(m.GetMessageTimestamp()), 0),
	}}, nil
}

func TestOnDemandHistoryMessages(t *testing.T) {
	data := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
		Conversations: []*waHistorySync.Conversation{{
			ID:       proto.String("5511999999999@s.whatsapp.net"),
			Messages: []*waHistorySync.HistorySyncMsg{webMsg("c", 300), webMsg("bad", 250), webMsg("a", 100), {}},
		}},
	}
	errs := 0
	got := onDemandHistoryMessages(data, fakeParse, func(error) { errs++ })
	if len(got) != 2 || got[0].Info.ID != "a" || got[1].Info.ID != "c" {
		t.Fatalf("ordem/filtragem errada: %+v", got)
	}
	if errs != 1 {
		t.Fatalf("esperava 1 erro, veio %d", errs)
	}
	if got[0].SourceWebMsg == nil || got[0].Info.Chat.User != "5511999999999" {
		t.Fatalf("mensagem sem marca de histórico ou chat errado")
	}

	data.SyncType = waHistorySync.HistorySync_RECENT.Enum()
	if got := onDemandHistoryMessages(data, fakeParse, func(error) {}); got != nil {
		t.Fatalf("só ON_DEMAND vira mensagem")
	}
}
