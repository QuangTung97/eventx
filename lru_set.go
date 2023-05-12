package eventx

type emptyStruct = struct{}

type lruEventSet struct {
	idSet   map[uint64]emptyStruct
	maxSize uint64
	idList  []uint64
	begin   uint64
	end     uint64
}

func newLRUEventSet(maxSize uint64) *lruEventSet {
	return &lruEventSet{
		idSet:   map[uint64]struct{}{},
		maxSize: maxSize,
		idList:  make([]uint64, maxSize),
	}
}

func (s *lruEventSet) computeIndex(pos uint64) uint64 {
	return pos % s.maxSize
}

func (s *lruEventSet) addID(id uint64) {
	s.idSet[id] = struct{}{}

	size := s.end - s.begin
	if size >= s.maxSize {
		index := s.computeIndex(s.begin)
		delete(s.idSet, s.idList[index])
		s.begin++
	}

	index := s.computeIndex(s.end)
	s.idList[index] = id
	s.end++
}

func (s *lruEventSet) existed(id uint64) bool {
	_, ok := s.idSet[id]
	return ok
}
