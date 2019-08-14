import json
from model import YandexAPIClient

if __name__ == '__main__':
	app_config_file = open('__test_configs/yandex_app_client.json')
	app_config = json.load(app_config_file)
	app_config_file.close()
	client_credentials_file = open('__test_configs/yandex_oauth_data.json')
	client_credentials = json.load(client_credentials_file)
	client_credentials_file.close()

	ya = YandexAPIClient(app_config, client_credentials)
	print(ya.getCountersList())