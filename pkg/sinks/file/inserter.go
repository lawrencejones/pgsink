package file

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/changelog/serialize"
	"github.com/pkg/errors"
)

type inserter struct {
	file       *os.File
	serializer serialize.Serializer
	sync.Mutex
}

func (s *inserter) Insert(ctx context.Context, modifications []*changelog.Modification) (count int, lsn *uint64, err error) {
	var buffer bytes.Buffer
	for _, modification := range modifications {
		count++
		if modification.LSN != nil {
			if lsn == nil || *lsn < *modification.LSN {
				lsn = modification.LSN
			}
		}

		bytes, err := s.serializer.Marshal(modification)
		if err != nil {
			return count, lsn, errors.Wrap(err, "failed to marshal modification")
		}

		if _, err := fmt.Fprintln(&buffer, string(bytes)); err != nil {
			return count, lsn, errors.Wrap(err, "failed to write to buffer")
		}
	}

	s.Lock()
	defer s.Unlock()

	_, err = s.file.Write(buffer.Bytes())
	return count, lsn, errors.Wrap(err, "failed to write modification")
}
