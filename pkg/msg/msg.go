package msg

const (
	MainFailure = "Main instance is down"

	PutSuccess        = "Added successfully"
	UpdateSuccess     = "Updated successfully"
	GetSuccess        = "Retrieved successfully"
	DeleteSuccess     = "Deleted successfully"
	ViewChangeSuccess = "View change successful"

	FailedToParse = "Failed to parse request body"
	KeyMissing    = "Key is missing"
	KeyDNE        = "Key does not exist"
	KeyTooLong    = "Key is too long"
	ValueMissing  = "Value is missing"
)
