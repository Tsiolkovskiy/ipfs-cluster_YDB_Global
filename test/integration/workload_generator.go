package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// GenerateWorkload creates a stream of workload operations based on the configured pattern
func (wg *WorkloadGenerator) GenerateWorkload(ctx context.Context) <-chan *WorkloadOperation {
	opChan := make(chan *WorkloadOperation, 1000) // Buffered channel for better performance
	
	go func() {
		defer close(opChan)
		
		switch wg.config.AccessPattern {
		case AccessPatternSequential:
			wg.generateSequentialWorkload(ctx, opChan)
		case AccessPatternRandom:
			wg.generateRandomWorkload(ctx, opChan)
		case AccessPatternBurst:
			wg.generateBurstWorkload(ctx, opChan)
		case AccessPatternHotCold:
			wg.generateHotColdWorkload(ctx, opChan)
		case AccessPatternZipfian:
			wg.generateZipfianWorkload(ctx, opChan)
		}
	}()
	
	return opChan
}

// generateSequentialWorkload creates sequential access pattern
func (wg *WorkloadGenerator) generateSequentialWorkload(ctx context.Context, opChan chan<- *WorkloadOperation) {
	operationCount := int64(0)
	batchCount := 0
	
	for operationCount < wg.config.TotalPins {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// Create batch of sequential operations
		batch := make([]cid.Cid, 0, wg.config.BatchSize)
		for i := 0; i < wg.config.BatchSize && operationCount < wg.config.TotalPins; i++ {
			cidValue := wg.generateSequentialCID(operationCount)
			batch = append(batch, cidValue)
			operationCount++
		}
		
		// Send bulk pin operation
		op := &WorkloadOperation{
			Type:      OpTypeBulkPin,
			BatchCIDs: batch,
			Timestamp: time.Now(),
			Priority:  1,
		}
		
		select {
		case opChan <- op:
		case <-ctx.Done():
			return
		}
		
		batchCount++
		
		// Add small delay to prevent overwhelming the system
		if batchCount%100 == 0 {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// generateRandomWorkload creates random access pattern
func (wg *WorkloadGenerator) generateRandomWorkload(ctx context.Context, opChan chan<- *WorkloadOperation) {
	operationCount := int64(0)
	
	for operationCount < wg.config.TotalPins {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// Randomly choose operation type
		opType := wg.getRandomOperationType()
		
		switch opType {
		case OpTypePin:
			cidValue := wg.getRandomCID()
			op := &WorkloadOperation{
				Type:      OpTypePin,
				CID:       cidValue,
				Timestamp: time.Now(),
				Priority:  wg.rand.Intn(5) + 1,
			}
			
			select {
			case opChan <- op:
				operationCount++
			case <-ctx.Done():
				return
			}
			
		case OpTypeBulkPin:
			batchSize := wg.rand.Intn(wg.config.BatchSize) + 1
			batch := make([]cid.Cid, 0, batchSize)
			
			for i := 0; i < batchSize && operationCount < wg.config.TotalPins; i++ {
				batch = append(batch, wg.getRandomCID())
				operationCount++
			}
			
			op := &WorkloadOperation{
				Type:      OpTypeBulkPin,
				BatchCIDs: batch,
				Timestamp: time.Now(),
				Priority:  2,
			}
			
			select {
			case opChan <- op:
			case <-ctx.Done():
				return
			}
			
		case OpTypeQuery:
			cidValue := wg.getRandomExistingCID()
			op := &WorkloadOperation{
				Type:      OpTypeQuery,
				CID:       cidValue,
				Timestamp: time.Now(),
				Priority:  3,
			}
			
			select {
			case opChan <- op:
			case <-ctx.Done():
				return
			}
		}
		
		// Random delay between operations
		delay := time.Duration(wg.rand.Intn(1000)) * time.Microsecond
		time.Sleep(delay)
	}
}

// generateBurstWorkload creates burst access pattern
func (wg *WorkloadGenerator) generateBurstWorkload(ctx context.Context, opChan chan<- *WorkloadOperation) {
	operationCount := int64(0)
	
	for operationCount < wg.config.TotalPins {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// Generate burst of operations
		burstSize := wg.config.BurstIntensity
		for i := 0; i < burstSize && operationCount < wg.config.TotalPins; i++ {
			cidValue := wg.getRandomCID()
			op := &WorkloadOperation{
				Type:      OpTypePin,
				CID:       cidValue,
				Timestamp: time.Now(),
				Priority:  5, // High priority for burst operations
			}
			
			select {
			case opChan <- op:
				operationCount++
			case <-ctx.Done():
				return
			}
		}
		
		// Wait for burst interval
		select {
		case <-time.After(wg.config.BurstInterval):
		case <-ctx.Done():
			return
		}
	}
}

// generateHotColdWorkload creates hot/cold data access pattern
func (wg *WorkloadGenerator) generateHotColdWorkload(ctx context.Context, opChan chan<- *WorkloadOperation) {
	operationCount := int64(0)
	
	for operationCount < wg.config.TotalPins {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		var cidValue cid.Cid
		var opType OperationType
		
		// 80% of operations target hot data (20% of total data)
		if wg.rand.Float64() < 0.8 {
			cidValue = wg.getHotDataCID()
			opType = OpTypeQuery // Hot data is mostly queried
		} else {
			cidValue = wg.getColdDataCID()
			opType = OpTypePin // Cold data is mostly new pins
		}
		
		op := &WorkloadOperation{
			Type:      opType,
			CID:       cidValue,
			Timestamp: time.Now(),
			Priority:  1,
		}
		
		select {
		case opChan <- op:
			if opType == OpTypePin {
				operationCount++
			}
		case <-ctx.Done():
			return
		}
		
		// Shorter delay for hot data access
		var delay time.Duration
		if opType == OpTypeQuery {
			delay = time.Duration(wg.rand.Intn(100)) * time.Microsecond
		} else {
			delay = time.Duration(wg.rand.Intn(1000)) * time.Microsecond
		}
		time.Sleep(delay)
	}
}

// generateZipfianWorkload creates Zipfian distribution access pattern
func (wg *WorkloadGenerator) generateZipfianWorkload(ctx context.Context, opChan chan<- *WorkloadOperation) {
	operationCount := int64(0)
	zipfGen := NewZipfianGenerator(len(wg.cidCache), 0.99) // Zipfian parameter
	
	for operationCount < wg.config.TotalPins {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// Get CID based on Zipfian distribution
		index := zipfGen.Next()
		var cidValue cid.Cid
		var opType OperationType
		
		if index < len(wg.cidCache) {
			cidValue = wg.cidCache[index]
			opType = OpTypeQuery
		} else {
			cidValue = wg.generateRandomCID()
			opType = OpTypePin
			operationCount++
		}
		
		op := &WorkloadOperation{
			Type:      opType,
			CID:       cidValue,
			Timestamp: time.Now(),
			Priority:  1,
		}
		
		select {
		case opChan <- op:
		case <-ctx.Done():
			return
		}
		
		time.Sleep(time.Duration(wg.rand.Intn(500)) * time.Microsecond)
	}
}

// PreGenerateCIDs pre-generates a set of CIDs for testing
func (wg *WorkloadGenerator) PreGenerateCIDs(count int) error {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	
	wg.cidCache = make([]cid.Cid, 0, count)
	
	for i := 0; i < count; i++ {
		cidValue := wg.generateRandomCID()
		wg.cidCache = append(wg.cidCache, cidValue)
	}
	
	return nil
}

// InitializeHotDataSet creates a set of frequently accessed CIDs
func (wg *WorkloadGenerator) InitializeHotDataSet() {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	
	hotDataCount := int(float64(len(wg.cidCache)) * wg.config.HotDataPercentage)
	
	for i := 0; i < hotDataCount && i < len(wg.cidCache); i++ {
		wg.hotDataSet[wg.cidCache[i].String()] = true
	}
}

// Helper methods for CID generation and selection

func (wg *WorkloadGenerator) generateSequentialCID(sequence int64) cid.Cid {
	// Create deterministic CID based on sequence number
	data := fmt.Sprintf("sequential-data-%d", sequence)
	hash, _ := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}

func (wg *WorkloadGenerator) generateRandomCID() cid.Cid {
	// Generate random CID
	randomData := make([]byte, 32)
	rand.Read(randomData)
	hash, _ := multihash.Sum(randomData, multihash.SHA2_256, -1)
	return cid.NewCidV1(cid.Raw, hash)
}

func (wg *WorkloadGenerator) getRandomCID() cid.Cid {
	wg.mu.RLock()
	defer wg.mu.RUnlock()
	
	if len(wg.cidCache) > 0 && wg.rand.Float64() < 0.3 {
		// 30% chance to use existing CID
		index := wg.rand.Intn(len(wg.cidCache))
		return wg.cidCache[index]
	}
	
	return wg.generateRandomCID()
}

func (wg *WorkloadGenerator) getRandomExistingCID() cid.Cid {
	wg.mu.RLock()
	defer wg.mu.RUnlock()
	
	if len(wg.cidCache) > 0 {
		index := wg.rand.Intn(len(wg.cidCache))
		return wg.cidCache[index]
	}
	
	return wg.generateRandomCID()
}

func (wg *WorkloadGenerator) getHotDataCID() cid.Cid {
	wg.mu.RLock()
	defer wg.mu.RUnlock()
	
	// Select from hot data set
	hotDataCount := int(float64(len(wg.cidCache)) * wg.config.HotDataPercentage)
	if hotDataCount > 0 {
		index := wg.rand.Intn(hotDataCount)
		return wg.cidCache[index]
	}
	
	return wg.generateRandomCID()
}

func (wg *WorkloadGenerator) getColdDataCID() cid.Cid {
	// Always generate new CID for cold data
	return wg.generateRandomCID()
}

func (wg *WorkloadGenerator) getRandomOperationType() OperationType {
	// Weighted random selection of operation types
	r := wg.rand.Float64()
	
	switch {
	case r < 0.6: // 60% pin operations
		return OpTypePin
	case r < 0.8: // 20% bulk pin operations
		return OpTypeBulkPin
	case r < 0.95: // 15% query operations
		return OpTypeQuery
	default: // 5% unpin operations
		return OpTypeUnpin
	}
}

// ZipfianGenerator generates numbers following Zipfian distribution
type ZipfianGenerator struct {
	n     int     // Number of items
	theta float64 // Zipfian parameter
	alpha float64 // Computed alpha
	zeta2 float64 // Zeta(2, theta)
	eta   float64 // Computed eta
}

// NewZipfianGenerator creates a new Zipfian generator
func NewZipfianGenerator(n int, theta float64) *ZipfianGenerator {
	zg := &ZipfianGenerator{
		n:     n,
		theta: theta,
	}
	
	zg.alpha = 1.0 / (1.0 - theta)
	zg.zeta2 = zg.zeta(2)
	zg.eta = (1.0 - math.Pow(2.0/float64(n), 1.0-theta)) / (1.0 - zg.zeta2/zg.zeta(n))
	
	return zg
}

// Next returns the next number in the Zipfian sequence
func (zg *ZipfianGenerator) Next() int {
	u := rand.Float64()
	uz := u * zg.zeta(zg.n)
	
	if uz < 1.0 {
		return 0
	}
	
	if uz < 1.0+math.Pow(0.5, zg.theta) {
		return 1
	}
	
	return int(float64(zg.n) * math.Pow(zg.eta*u-zg.eta+1.0, zg.alpha))
}

// zeta computes the zeta function
func (zg *ZipfianGenerator) zeta(n int) float64 {
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), zg.theta)
	}
	return sum
}