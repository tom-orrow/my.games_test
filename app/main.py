from fastapi import FastAPI

from fastapi.responses import RedirectResponse
from app.db import DB


app = FastAPI(title="Test project for my.games")


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.get("/top_worst_actors")
async def get_top_worst_actors():
    return DB().get_top_worst_actors()


@app.get("/top_producers_by_number_of_movies")
async def get_top_producers_by_number_of_movies():
    return DB().get_top_producers_by_number_of_movies()


@app.get("/most_popular_genres")
async def get_most_popular_genres():
    return DB().get_most_popular_genres()


@app.get("/movies_top250")
async def get_movies_top250():
    return DB().get_movies_top250()


@app.on_event("startup")
async def startup():
    DB()
