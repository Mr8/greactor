package reactor

type Response struct {
	ActionId string                 `json:"action_id"`
	Command  string                 `json:"command"`
	Services string                 `json:"services"`
	Errno    int                    `json:"errno"`
	Errmsg   string                 `json:"errmsg"`
	Data     map[string]interface{} `json:"data"`
}
