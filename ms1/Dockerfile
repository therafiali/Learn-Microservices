FROM python:3.12

RUN pip install poetry

WORKDIR /code

COPY . /code/

RUN poetry config virtualenvs.create false

RUN poetry install

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "ms1.main:app", "--host", "0.0.0.0", "--reload"]