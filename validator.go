package main

import (
	"fmt"
	"github.com/allegro/akubra/log"
	"gopkg.in/validator.v1"
	"reflect"
)

// ValidateBrimConfig Brim Yaml values validation
func ValidateBrimConfig(bc BrimConf) bool {
	validator.SetValidationFunc("adminsConfValidator", adminsConfValidator)
	valid, validationErrors := validator.Validate(bc)
	fmt.Printf("BrimConf Validate - valid: %v, errors: %v\n", valid, validationErrors)
	for propertyName, validatorMessage := range validationErrors {
		log.Printf("[ ERROR ] BRIM YAML config validation -> propertyName: '%s', validatorMessage: '%s'\n", propertyName, validatorMessage)
	}

	return valid
}

// adminsConfValidator for "admins" section in brim Yaml configuration
func adminsConfValidator(v interface{}, param string) error {
	properties := []string{"AdminAccessKey", "AdminSecretKey", "AdminPrefix", "Endpoint"}
	msgPfx := "AdminConfValidator: "
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Map {
		validateCredentialProperty := func(s3credentials reflect.Value, idx int, propertyName string) bool {
			propertyValue := s3credentials.Index(idx).FieldByName(propertyName).String()
			if len(propertyValue) < 1 {
				return false
			}
			return true
		}

		if val.Len() == 0 {
			return fmt.Errorf("%sEmpty admins section - param: %q", msgPfx, param)
		}
		for _, key := range val.MapKeys() {
			s3credentials := val.MapIndex(key)
			if key.Len() < 3 {
				return fmt.Errorf("%sWrong region name - param: %q", msgPfx, param)
			}
			if s3credentials.Len() < 2 {
				return fmt.Errorf("%sCount of regions must be greather then one - param: %q",
					msgPfx, param)
			}
			for idx := 0; idx < s3credentials.Len(); idx++ {
				listingElem := s3credentials.Index(idx).Type()
				if listingElem.String() != "main.AdminConf" {
					return fmt.Errorf("%sWrong credentials structure "+
						"(required: main.AdminConf - param: %q", msgPfx, param)
				}
				var isValid bool
				for _, propertyName := range properties {
					isValid = validateCredentialProperty(s3credentials, idx, propertyName)
					if !isValid {
						return fmt.Errorf("%sEmpty or not-exists credential '%s' (in region %q) - param: %q",
							msgPfx, propertyName, key, param)
					}
				}
			}
		}
	} else {
		return fmt.Errorf("%svalidates only Map kind", msgPfx)
	}
	return nil
}
