down: 
	docker compose down -v

up:
	docker compose up -d

ui: 
	cmd /c start "http://localhost:9021"

stop: down 

run: down up 