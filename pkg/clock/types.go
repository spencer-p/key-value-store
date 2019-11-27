package clock

type Clock interface {
	Compare(Clock) CompareResult
	Increment(string)
	Copy() Clock
}

type CompareResult int

const (
	NoRelation CompareResult = iota
	Less
	Equal
	Greater
)
