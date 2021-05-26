package eventx

type sampleEvent struct {
	seq uint64
}

// UnmarshalledEvent for user defined events
type UnmarshalledEvent sampleEvent

func unmarshalEvent(e Event) UnmarshalledEvent {
	return UnmarshalledEvent(sampleEvent{
		seq: e.Seq,
	})
}
