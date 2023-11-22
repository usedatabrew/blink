package config

import "github.com/go-playground/validator/v10"

func ValidateConfigSchema(config Configuration) {
	validate := validator.New()
	err := validate.Struct(config)
	if err == nil {
		return
	}
	validationErrors := err.(validator.ValidationErrors)
	if len(validationErrors) > 0 {
		panic(validationErrors)
	}
}
