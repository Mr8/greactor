package reactor

import (
	log "github.com/alecthomas/log4go"
)

type AppConfig struct {
	Port       int
	Host       string
	DB         int
	Password   string // the RPC MQ(redis) password
	Event      string // listen event, it's a LIST key in Redis
	AuthKey    string // the RPC authtication key
	HandlerMap map[string]func(*Request) (*Response, error)
}

func NewAppConfig(
	port int, host string, db int, event string, password string, auth_key string,
	handlermap map[string]func(*Request) (*Response, error)) *AppConfig {

	_appconfig := &AppConfig{
		Port:       port,
		Host:       host,
		DB:         db,
		Password:   password,
		Event:      event,
		AuthKey:    auth_key,
		HandlerMap: handlermap}
	return _appconfig
}

type App struct {
	event  string
	config *AppConfig
	router *Router
}

func NewApp(config *AppConfig) *App {
	if err := InitSubscriber(
		config.Host, config.Port, config.DB, config.Password); err != nil {
		log.Error("Create APP failed, error:%s", err)
	} else {
		router := NewRouter(Suber, config.AuthKey, config.HandlerMap)
		app := &App{event: config.Event, config: config, router: router}
		return app
	}
	return nil
}

func (app *App) Start() {
	app.router.Run(app.event)
}

func (app *App) Close() {
	app.router.Close()
}
