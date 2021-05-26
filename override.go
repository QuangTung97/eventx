package eventx

type sampleEvent struct {
	id  uint64
	seq uint64
}

// UnmarshalledEvent for user defined events
type UnmarshalledEvent sampleEvent

func unmarshalEvent(e Event) UnmarshalledEvent {
	return UnmarshalledEvent(sampleEvent{
		id:  e.ID,
		seq: e.Seq,
	})
}

func (e UnmarshalledEvent) getSequence() uint64 {
	return e.seq
}
