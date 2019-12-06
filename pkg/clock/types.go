package clock

type CompareResult int

const (
	NoRelation CompareResult = iota
	Less
	Equal
	Greater
)
