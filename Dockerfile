FROM python:3.10-bookworm

RUN useradd --create-home dfltuser
WORKDIR /home/dfltuser
USER dfltuser

COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir data

COPY main.py .

CMD [ "python", "main.py" ]