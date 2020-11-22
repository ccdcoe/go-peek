package mitre

import (
	"errors"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/utils"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
)

const src = `https://raw.githubusercontent.com/mitre/cti/master/enterprise-attack/enterprise-attack.json`

type Config struct {
	EnterpriseDump string
}

func (c *Config) Validate() error {
	if c.EnterpriseDump == "" {
		return errors.New("Mitre mapper missing dump file!")
	}
	return nil
}

type Mapper struct {
	c        Config
	Mappings map[string]meta.Technique
}

func NewMapper(c Config) (*Mapper, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var raw io.Reader
	if utils.FileNotExists(c.EnterpriseDump) {
		logrus.Tracef(
			"Mitre enterprise dump %s does not exists, downloading from %s",
			c.EnterpriseDump,
			src,
		)
		response, err := http.Get(src)
		if err != nil {
			return nil, err
		}
		raw = response.Body
		defer response.Body.Close()
	} else {
		f, err := os.Open(c.EnterpriseDump)
		if err != nil {
			return nil, err
		}
		raw = f
		defer f.Close()
	}
	data, err := ioutil.ReadAll(raw)
	if err != nil {
		return nil, err
	}
	// var mappings meta.Techniques
	_, err = parseEnterprise(data)
	return &Mapper{}, nil
}

func parseEnterprise(input []byte) (*meta.Techniques, error) {
	return nil, nil
}
