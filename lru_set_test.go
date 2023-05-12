package eventx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLRUSet(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		s := newLRUEventSet(100)

		s.addID(11)
		s.addID(12)
		s.addID(13)

		assert.Equal(t, false, s.existed(10))
		assert.Equal(t, true, s.existed(11))
		assert.Equal(t, true, s.existed(12))
		assert.Equal(t, true, s.existed(13))
		assert.Equal(t, false, s.existed(14))
	})

	t.Run("reach-limit", func(t *testing.T) {
		s := newLRUEventSet(3)

		s.addID(11)
		s.addID(12)
		s.addID(13)
		s.addID(14)

		assert.Equal(t, false, s.existed(10))
		assert.Equal(t, false, s.existed(11))
		assert.Equal(t, true, s.existed(12))
		assert.Equal(t, true, s.existed(14))
	})

	t.Run("wrap-around", func(t *testing.T) {
		s := newLRUEventSet(3)

		s.addID(11)
		s.addID(12)
		s.addID(13)

		s.addID(14)
		s.addID(15)
		s.addID(16)

		s.addID(17)
		s.addID(18)
		s.addID(19)

		assert.Equal(t, false, s.existed(11))
		assert.Equal(t, false, s.existed(12))
		assert.Equal(t, false, s.existed(13))

		assert.Equal(t, false, s.existed(14))
		assert.Equal(t, false, s.existed(15))
		assert.Equal(t, false, s.existed(16))

		assert.Equal(t, true, s.existed(17))
		assert.Equal(t, true, s.existed(18))
		assert.Equal(t, true, s.existed(19))

		assert.Equal(t, false, s.existed(20))
	})
}
