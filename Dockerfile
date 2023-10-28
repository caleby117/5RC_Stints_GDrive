# syntax=docker/dockerfile:1

FROM alpine:latest as build
RUN apk add git make g++
RUN git clone -b linux_compatibility https://github.com/caleby117/5RC_Stint_Time_Util.git && cd 5RC_Stint_Time_Util
WORKDIR 5RC_Stint_Time_Util
RUN cd src/5RC_Stint_Time_Util/ && make

FROM python:3.9.18-alpine3.18
WORKDIR .
COPY ./src ./src
COPY ./requirements.txt ./requirements.txt
COPY --from=build /5RC_Stint_Time_Util/src/5RC_Stint_Time_Util/elf/5RC_Stint_Time_Util ./telem/util
COPY --from=build /5RC_Stint_Time_Util/src/5RC_Stint_Time_Util/SampleVars.txt ./telem/SampleVars.txt
COPY ./creds ./creds
RUN pip install -r requirements.txt
CMD ["python3", "src/main.py"]
