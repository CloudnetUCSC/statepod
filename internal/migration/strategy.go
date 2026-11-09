package migration

import (
	"context"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// Strategy defines an interface for different container migration methods
type Strategy interface {
	// Execute perms the migration to the specified destination
	Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error
}
