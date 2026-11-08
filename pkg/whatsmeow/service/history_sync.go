package whatsmeow_service

import (
	"sort"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// historyParser converte uma WebMessageInfo do histórico no mesmo evento de uma mensagem ao vivo.
type historyParser func(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error)

// onDemandHistoryMessages devolve, da mais antiga para a mais nova, as mensagens de um
// histórico pedido sob demanda (chat/history-sync). Elas seguem pelo mesmo caminho do
// webhook de mensagem, marcadas como histórico (SourceWebMsg != nil).
func onDemandHistoryMessages(data *waHistorySync.HistorySync, parse historyParser, onErr func(error)) []*events.Message {
	if data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
		return nil
	}
	var out []*events.Message
	for _, conv := range data.GetConversations() {
		chat, err := types.ParseJID(conv.GetID())
		if err != nil {
			onErr(err)
			continue
		}
		for _, hm := range conv.GetMessages() {
			if hm.GetMessage() == nil {
				continue
			}
			evt, err := parse(chat, hm.GetMessage())
			if err != nil {
				onErr(err)
				continue
			}
			if evt.SourceWebMsg == nil {
				evt.SourceWebMsg = hm.GetMessage()
			}
			out = append(out, evt)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Info.Timestamp.Before(out[j].Info.Timestamp) })
	return out
}
