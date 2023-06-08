package snow

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

type SnowflakeSql struct {
	Sql      string
	FileName string
}

func LoadSqls(folder string) ([]SnowflakeSql, error) {
	dirFS := os.DirFS(folder)
	entries, err := fs.ReadDir(dirFS, ".")
	if err != nil {
		return nil, err
	}

	snowSqls := make([]SnowflakeSql, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		if !strings.EqualFold(filepath.Ext(fileName), ".sql") {
			logrus.Debugf("skip non-sql file: %s", fileName)
			continue
		}

		rawQuery, err := fs.ReadFile(dirFS, fileName)
		if err != nil {
			return nil, err
		}

		snowSqls = append(snowSqls, SnowflakeSql{
			Sql:      string(rawQuery),
			FileName: fileName,
		})
	}
	return snowSqls, nil
}
