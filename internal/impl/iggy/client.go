package iggy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	iggyclient "github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/warpstreamlabs/bento/public/service"
)

// iggyClient is the part of the Iggy SDK client the input uses. The SDK's
// iggcon.Client satisfies it; tests substitute a fake.
type iggyClient interface {
	GetTopic(ctx context.Context, streamID, topicID iggcon.Identifier) (*iggcon.TopicDetails, error)
	GetConsumerGroup(ctx context.Context, streamID, topicID, groupID iggcon.Identifier) (*iggcon.ConsumerGroupDetails, error)
	CreateConsumerGroup(ctx context.Context, streamID, topicID iggcon.Identifier, name string) (*iggcon.ConsumerGroupDetails, error)
	JoinConsumerGroup(ctx context.Context, streamID, topicID, groupID iggcon.Identifier) error
	LeaveConsumerGroup(ctx context.Context, streamID, topicID, groupID iggcon.Identifier) error
	SyncConsumerGroup(ctx context.Context, streamID, topicID, groupID iggcon.Identifier) (*iggcon.ConsumerGroupAssignment, error)
	GetConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamID, topicID iggcon.Identifier, partitionID *uint32) (*iggcon.ConsumerOffsetInfo, error)
	StoreConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamID, topicID iggcon.Identifier, offset uint64, partitionID *uint32) error
	PollMessages(ctx context.Context, streamID, topicID iggcon.Identifier, consumer iggcon.Consumer, strategy iggcon.PollingStrategy, count uint32, autoCommit bool, partitionID *uint32) (*iggcon.PolledMessage, error)
	Close() error
}

type tlsConfig struct {
	enabled        bool
	caFile         string
	serverName     string
	skipCertVerify bool
}

// dialSeeds connects to the first seed address that accepts a signed-in
// session. The SDK then learns the rest of the cluster roster from that node
// and follows leader changes on its own; the seeds only matter for the first
// connection and for the fresh dial after a torn-down connection.
func dialSeeds(ctx context.Context, addresses []string, username, password string, tlsConf tlsConfig, log *service.Logger) (iggyClient, error) {
	var errs []error
	for _, address := range addresses {
		opts := []tcp.Option{
			tcp.WithServerAddress(address),
			tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(username, password)),
		}
		if tlsConf.enabled {
			opts = append(opts, tcp.WithTLS(
				tcp.WithTLSCAFile(tlsConf.caFile),
				tcp.WithTLSDomain(tlsConf.serverName),
				tcp.WithTLSValidateCertificate(!tlsConf.skipCertVerify),
			))
		}
		cl, err := iggyclient.NewIggyClient(
			iggyclient.WithTcp(opts...),
			iggyclient.WithLogger(slog.New(&slogAdapter{log: log})),
		)
		if err != nil {
			return nil, err
		}
		// The SDK retries a refused dial forever by default, so each seed gets
		// a bounded slice of the caller's budget before the next one is tried.
		dialCtx, cancel := context.WithTimeout(ctx, seedDialTimeout)
		err = cl.Connect(dialCtx)
		cancel()
		if err == nil {
			return cl, nil
		}
		_ = cl.Close()
		errs = append(errs, fmt.Errorf("%s: %w", address, err))
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("no iggy seed address accepted a connection: %w", errors.Join(errs...))
}

const seedDialTimeout = 10 * time.Second

// isMembershipError reports whether the server no longer counts this client
// as the owner of what it polled: the client identity changed (every SDK
// reconnect registers a new one) or the group rebalanced.
func isMembershipError(err error) bool {
	return errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) ||
		errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned)
}

func isGroupNotFound(err error) bool {
	return errors.Is(err, ierror.ErrConsumerGroupIdNotFound) ||
		errors.Is(err, ierror.ErrConsumerGroupNameNotFound)
}

//------------------------------------------------------------------------------

// slogAdapter forwards the SDK's warnings and errors to the Bento logger.
// Its info and debug output (per-request routing chatter) is dropped.
type slogAdapter struct {
	log   *service.Logger
	attrs []slog.Attr
}

func (s *slogAdapter) Enabled(_ context.Context, level slog.Level) bool {
	return s.log != nil && level >= slog.LevelWarn
}

func (s *slogAdapter) Handle(_ context.Context, r slog.Record) error {
	var msg strings.Builder
	msg.WriteString("iggy client: " + r.Message)
	for _, a := range s.attrs {
		msg.WriteString(" " + a.String())
	}
	r.Attrs(func(a slog.Attr) bool {
		msg.WriteString(" " + a.String())
		return true
	})
	if r.Level >= slog.LevelError {
		s.log.Error(msg.String())
	} else {
		s.log.Warn(msg.String())
	}
	return nil
}

func (s *slogAdapter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &slogAdapter{log: s.log, attrs: append(append([]slog.Attr{}, s.attrs...), attrs...)}
}

func (s *slogAdapter) WithGroup(string) slog.Handler { return s }
