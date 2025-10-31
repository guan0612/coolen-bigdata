# 使用 Python 官方映像作為基底
FROM python:3.8

# 設定時區為台北
ENV TZ=Asia/Taipei
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 設定工作目錄
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

# 安裝相依套件
RUN pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 5002

# 複製程式碼進入容器中
COPY . /app/

# 預設執行指令
CMD ["python", "main.py"]