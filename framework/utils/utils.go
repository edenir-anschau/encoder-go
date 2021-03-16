package utils

import "encoding/json"

func IsJson(s []byte) error {
	var js struct{}

	if err := json.Unmarshal(s, &js); err != nil {
		return err
	}

	return nil
}
