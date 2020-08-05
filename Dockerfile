FROM python:3.6
RUN pip install --upgrade pip
RUN pip install aiohttp==3.6.2 requests==2.22.0 redis==3.4.0 aiofiles==0.5.0 motor==2.1.0 python_dateutil==2.8.1
COPY ./ /server/
WORKDIR /server/
ENTRYPOINT ["python", "run.py"]
