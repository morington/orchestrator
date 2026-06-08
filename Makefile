restart:
	docker compose down && \
	docker compose up -d --build && \
	cd /home/amorington/pfl/processors/processor-message-collector && \
	docker compose down && docker compose up -d && \
	cd /home/amorington/pfl/processors/processor-telegram-output && \
	docker compose down && docker compose up -d && \
	cd /home/amorington/pfl/processors/processor-translator && \
	docker compose down && docker compose up -d
