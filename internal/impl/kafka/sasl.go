package kafka

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/public/service"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func saslField() *service.ConfigField {
	return service.NewObjectListField("sasl",
		service.NewStringAnnotatedEnumField("mechanism", map[string]string{
			"none":          "Disable sasl authentication",
			"PLAIN":         "Plain text authentication.",
			"OAUTHBEARER":   "OAuth Bearer based authentication.",
			"SCRAM-SHA-256": "SCRAM based authentication as specified in RFC5802.",
			"SCRAM-SHA-512": "SCRAM based authentication as specified in RFC5802.",
		}).
			Description("The SASL mechanism to use."),
		service.NewStringField("username").
			Description("A username to provide for PLAIN or SCRAM-* authentication.").
			Default(""),
		service.NewStringField("password").
			Description("A password to provide for PLAIN or SCRAM-* authentication.").
			Default("").Secret(),
		service.NewStringField("token").
			Description("The token to use for a single session's OAUTHBEARER authentication.").
			Default(""),
		service.NewObjectField("oauth2",
			service.NewBoolField("enabled").
				Description("Whether to use OAuth version 2 in requests.").
				Default(false),
			service.NewStringField("client_key").
				Description("A value used to identify the client to the token provider.").
				Default(""),
			service.NewStringField("client_secret").
				Description("A secret used to establish ownership of the client key.").
				Default("").Secret(),
			service.NewURLField("token_url").
				Description("The URL of the token provider.").
				Default(""),
			service.NewStringListField("scopes").
				Description("A list of optional requested permissions.").
				Default([]any{}).
				Advanced(),
			service.NewAnyMapField("endpoint_params").
				Description("A list of optional endpoint parameters, values should be arrays of strings.").
				Advanced().
				Optional(),
		).
			Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
			Optional().Version("1.18.0").Advanced(),
		service.NewStringMapField("extensions").
			Description("Key/value pairs to add to OAUTHBEARER authentication requests.").
			Optional(),
	).
		Description("Specify one or more methods of SASL authentication. SASL is tried in order; if the broker supports the first mechanism, all connections will use that mechanism. If the first mechanism fails, the client will pick the first supported mechanism. If the broker does not support any client mechanisms, connections will fail.").
		Advanced().Optional().
		Example(
			[]any{
				map[string]any{
					"mechanism": "SCRAM-SHA-512",
					"username":  "foo",
					"password":  "bar",
				},
			},
		)
}

func saslMechanismsFromConfig(c *service.ParsedConfig) ([]sasl.Mechanism, error) {
	if !c.Contains("sasl") {
		return nil, nil
	}

	sList, err := c.FieldObjectList("sasl")
	if err != nil {
		return nil, err
	}

	var mechanisms []sasl.Mechanism
	var mechanism sasl.Mechanism
	for i, mConf := range sList {
		mechStr, err := mConf.FieldString("mechanism")
		if err == nil {
			switch mechStr {
			case "", "none":
				continue
			case "PLAIN":
				mechanism, err = plainSaslFromConfig(mConf)
				mechanisms = append(mechanisms, mechanism)
			case "OAUTHBEARER":
				mechanism, err = oauthSaslFromConfig(mConf)
				mechanisms = append(mechanisms, mechanism)
			case "SCRAM-SHA-256":
				mechanism, err = scram256SaslFromConfig(mConf)
				mechanisms = append(mechanisms, mechanism)
			case "SCRAM-SHA-512":
				mechanism, err = scram512SaslFromConfig(mConf)
				mechanisms = append(mechanisms, mechanism)
			default:
				err = fmt.Errorf("unknown mechanism: %v", mechStr)
			}
		}
		if err != nil {
			if len(sList) == 1 {
				return nil, err
			}
			return nil, fmt.Errorf("mechanism %v: %w", i, err)
		}
	}

	return mechanisms, nil
}

func plainSaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return plain.Plain(func(c context.Context) (plain.Auth, error) {
		return plain.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}

func oauthSaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	if c.Contains("oauth2") {
		if enabled, _ := c.FieldBool("oauth2", "enabled"); enabled {
			key, err := c.FieldString("oauth2", "client_key")
			if err != nil {
				return nil, err
			}
			secret, err := c.FieldString("oauth2", "client_secret")
			if err != nil {
				return nil, err
			}
			tokenURL, err := c.FieldString("oauth2", "token_url")
			if err != nil {
				return nil, err
			}
			scopes, err := c.FieldStringList("oauth2", "scopes")
			if err != nil {
				return nil, err
			}
			endpointParams := map[string][]string{}
			if c.Contains("oauth2", "endpoint_params") {
				params, err := c.FieldAnyMap("oauth2", "endpoint_params")
				if err != nil {
					return nil, err
				}
				for k, v := range params {
					if endpointParams[k], err = v.FieldStringList(); err != nil {
						return nil, err
					}
				}
			}
			var extensions map[string]string
			if c.Contains("oauth2", "extensions") {
				if extensions, err = c.FieldStringMap("extensions"); err != nil {
					return nil, err
				}
			}
			conf := &clientcredentials.Config{
				ClientID:       key,
				ClientSecret:   secret,
				TokenURL:       tokenURL,
				Scopes:         scopes,
				EndpointParams: endpointParams,
			}
			ts := oauth2.ReuseTokenSource(nil, conf.TokenSource(context.Background()))
			return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
				tok, err := ts.Token()
				if err != nil {
					return oauth.Auth{}, err
				}
				return oauth.Auth{
					Token:      tok.AccessToken,
					Extensions: extensions,
				}, nil
			}), nil
		}
	}

	token, err := c.FieldString("token")
	if err != nil {
		return nil, err
	}
	var extensions map[string]string
	if c.Contains("extensions") {
		if extensions, err = c.FieldStringMap("extensions"); err != nil {
			return nil, err
		}
	}
	return oauth.Oauth(func(c context.Context) (oauth.Auth, error) {
		return oauth.Auth{
			Token:      token,
			Extensions: extensions,
		}, nil
	}), nil
}

func scram256SaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return scram.Sha256(func(c context.Context) (scram.Auth, error) {
		return scram.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}

func scram512SaslFromConfig(c *service.ParsedConfig) (sasl.Mechanism, error) {
	username, err := c.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := c.FieldString("password")
	if err != nil {
		return nil, err
	}
	return scram.Sha512(func(c context.Context) (scram.Auth, error) {
		return scram.Auth{
			User: username,
			Pass: password,
		}, nil
	}), nil
}
