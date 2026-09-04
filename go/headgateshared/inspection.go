package headgateshared

// Shared upper bounds keep operational reads bounded consistently across stores.
const (
	InspectionSampleLimit         = 50_000
	InspectionPositionLimit       = 1_000
	InspectionQuietPartitionLimit = 1_000
	InspectionMaxPage             = 200
	InspectionMemorySampleLimit   = 1_000
)

func AgeMillis(nowMs, atMs int64) int64 {
	return max(nowMs-atMs, 0)
}

func TimeToDrainMillis(backlog int64, arrivalRate, drainRate float64) *int64 {
	if drainRate <= arrivalRate || drainRate <= 0 {
		return nil
	}
	value := int64(float64(backlog) / (drainRate - arrivalRate) * 1000)
	return &value
}
