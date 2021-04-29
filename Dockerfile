FROM python:3.9


WORKDIR /stream-app .

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ./ ./ 

CMD [ "python", "./main.py" ]
CMD [ "pytest" ]
CMD [ "pytest", "--cov" ]