# syntax=docker/dockerfile:1
ENV UTIL_SRC 5RC_Stint_Time_Util/src/5RC_Stint_Time_Util/

FROM alpine:latest as build
WORKDIR /build
RUN git clone https://github.com/caleby117/5RC_Stint_Time_Util.git && git checkout linux_compatibility
RUN cd src/5RC_Stint_Time_Util/ && make

FROM python:latest
WORKDIR .
RUN git clone https://github.com/caleby117/5RC_Stints_GDrive.git && cd 5RC_Stints_GDrive
COPY --from=build /build/$UITL/elf/5RC_Stint_Time_Util ./telem/util
COPY --from=build /build/$UTIL/SampleVars.txt ./telem/SampleVars.txt
COPY ./creds ./creds
RUN pip install -r requirements.txt
CMD ["python3", "src/main.py"]
