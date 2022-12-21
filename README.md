## Тестовое задание для my.games

#### Описание
Для тестового задания взял датасет imdb_data на 20млн строк с Kaggle.
В датасете хранятся склеенные данные по фильмам, названиям, актёрам.
Попробовал через docker поднять postgres + fastapi и разобрать этот датасет на реляционную модель. Обработка и заливка получилась дольше чем планировал и пока в качестве заглушки ограничился первыми 100к строками.

#### Датасет
https://www.kaggle.com/datasets/muhammadkaleemullah/imdb-data?select=complete_data.csv

Примерное описание структуры датасета: https://www.imdb.com/interfaces/

#### Подготовка
Датасет взят с kaggle. Для автоматизации скачивания нужно получить ключ api:
- Авторизоваться на kaggle.com
- Зайти в аккаунт, в разделе API кнопка Create New API Token
- Подготовить `.env` на основе `.env.example`, указав ключи из полученного JSON-а

#### Запуск контейнера
```
docker-compose up -d --build
```

#### Запуск разбора данных
```
docker-compose --env-file .env run web sh ./app/prepare_data.sh
docker-compose --env-file .env run web python3 ./app/seed_with_psycopg.py
```

#### FastAPI
http://localhost:8000/

Для аналитики сделал четыре эндпоинта:
- **/top_worst_actors** - топ 10 худших актёров по средней оценке их фильмов
- **/top_producers_by_number_of_movies** - топ 10 продюссеров по количеству снятых фильмов
- **/most_popular_genres** - популярность жанров по количеству фильмов
- **/movies_top250** - топ 250 фильмов по средней оценке среди тех, где больше 50к голосов