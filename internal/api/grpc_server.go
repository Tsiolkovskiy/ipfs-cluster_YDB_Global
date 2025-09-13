package api

import (
	"context"
	"net"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the gRPC API server
type GRPCServer struct {
	server   *grpc.Server
	services Services
	logger   *zap.Logger
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(services Services, logger *zap.Logger) *GRPCServer {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor(logger)),
	)

	grpcServer := &GRPCServer{
		server:   server,
		services: services,
		logger:   logger,
	}

	// Register gRPC services here when proto files are available
	// pb.RegisterGlobalDataControllerServer(server, grpcServer)

	return grpcServer
}

// Start starts the gRPC server
func (s *GRPCServer) Start(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.logger.Info("Starting gRPC server", zap.String("addr", addr))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server
func (s *GRPCServer) Stop(ctx context.Context) error {
	s.logger.Info("Stopping gRPC server")
	s.server.GracefulStop()
	return nil
}

// Health check implementation (when proto is available)
func (s *GRPCServer) Health(ctx context.Context) error {
	if s.services == nil {
		return status.Error(codes.Unavailable, "services not available")
	}
	return nil
}

// Example gRPC method implementations (to be replaced with actual proto-generated methods)

// GetZone implements the GetZone gRPC method
func (s *GRPCServer) GetZone(ctx context.Context, zoneID string) (*models.Zone, error) {
	if zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "zone ID is required")
	}

	zone, err := s.services.GetZone(ctx, zoneID)
	if err != nil {
		s.logger.Error("Failed to get zone", zap.String("zone_id", zoneID), zap.Error(err))
		return nil, status.Error(codes.NotFound, "zone not found")
	}

	return zone, nil
}

// ListZones implements the ListZones gRPC method
func (s *GRPCServer) ListZones(ctx context.Context) ([]*models.Zone, error) {
	zones, err := s.services.GetZones(ctx)
	if err != nil {
		s.logger.Error("Failed to list zones", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to list zones")
	}

	return zones, nil
}

// CreateZone implements the CreateZone gRPC method
func (s *GRPCServer) CreateZone(ctx context.Context, zone *models.Zone) (*models.Zone, error) {
	if zone == nil {
		return nil, status.Error(codes.InvalidArgument, "zone is required")
	}

	if err := s.validateZone(zone); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.services.CreateZone(ctx, zone); err != nil {
		s.logger.Error("Failed to create zone", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to create zone")
	}

	return zone, nil
}

// PinContent implements the PinContent gRPC method
func (s *GRPCServer) PinContent(ctx context.Context, request *PinRequest) (*models.PinPlan, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "pin request is required")
	}

	if err := s.validatePinRequest(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	plan, err := s.services.PinContent(ctx, request)
	if err != nil {
		s.logger.Error("Failed to pin content", zap.String("cid", request.CID), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to pin content")
	}

	return plan, nil
}

// GetContentStatus implements the GetContentStatus gRPC method
func (s *GRPCServer) GetContentStatus(ctx context.Context, cid string) (*models.PinStatus, error) {
	if cid == "" {
		return nil, status.Error(codes.InvalidArgument, "CID is required")
	}

	pinStatus, err := s.services.GetContentStatus(ctx, cid)
	if err != nil {
		s.logger.Error("Failed to get content status", zap.String("cid", cid), zap.Error(err))
		return nil, status.Error(codes.NotFound, "content not found")
	}

	return pinStatus, nil
}

// Validation methods (reused from HTTP gateway)
func (s *GRPCServer) validateZone(zone *models.Zone) error {
	if zone.Name == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	if zone.Region == "" {
		return status.Error(codes.InvalidArgument, "region is required")
	}
	if zone.Status != "" {
		switch zone.Status {
		case models.ZoneStatusActive, models.ZoneStatusDegraded, models.ZoneStatusMaintenance, models.ZoneStatusOffline:
			// Valid status
		default:
			return status.Errorf(codes.InvalidArgument, "invalid status: %s", zone.Status)
		}
	}
	return nil
}

func (s *GRPCServer) validatePinRequest(request *PinRequest) error {
	if request.CID == "" {
		return status.Error(codes.InvalidArgument, "CID is required")
	}
	if request.Size < 0 {
		return status.Error(codes.InvalidArgument, "size cannot be negative")
	}
	if request.Priority < 0 {
		return status.Error(codes.InvalidArgument, "priority cannot be negative")
	}
	return nil
}

// loggingInterceptor provides request logging for gRPC
func loggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		
		resp, err := handler(ctx, req)
		
		duration := time.Since(start)
		
		if err != nil {
			logger.Error("gRPC request failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			logger.Info("gRPC request completed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
			)
		}
		
		return resp, err
	}
}