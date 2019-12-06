package msg

const (
	MainFailure = "Main instance is down"

	PutSuccess               = "Added successfully"
	UpdateSuccess            = "Updated successfully"
	GetSuccess               = "Retrieved successfully"
	NumKeySuccess            = "Key count retrieved successfully"
	DeleteSuccess            = "Deleted successfully"
	ViewChangeSuccess        = "View change successful"
	PartialViewChangeSuccess = "Partial view change successful"
	ShardInfoSuccess         = "Shard information retrieved successfully"
	ShardMembSuccess         = "Shard membership retrieved successfully"

	FailedToParse = "Failed to parse request body"
	KeyMissing    = "Key is missing"
	KeyDNE        = "Key does not exist"
	KeyTooLong    = "Key is too long"
	ValueMissing  = "Value is missing"
	BadForwarding = "Bad forwarding address"
	Unavailable   = "Unable to satisfy request"
)
