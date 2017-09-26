package reactor

type Request struct {
	Version     string                 `json:"version"`
	Services    string                 `json:"services"`
	ActionId    string                 `json:"action_id"`
	Command     string                 `json:"command"`
	Payload     map[string]interface{} `json:"payload"`
	AuthKey     string                 `json:"auth_key"`
	ResponseKey string                 `json:"response_key"`
}
