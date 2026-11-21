FROM python:3.12-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY pipeline/*.cpp pipeline/*.hpp ./
# GCC profile from scripts/train_pgo.sh, trained in this image on amd64. A
# stale profile only skips the functions that changed.
COPY pipeline/pgo/ ./
RUN set -eu; \
    case "$TARGETARCH" in \
        amd64) cpu=-march=x86-64-v3; pgo="-fprofile-use -fprofile-correction -Wno-missing-profile -Wno-error=coverage-mismatch" ;; \
        *) cpu=; pgo= ;; \
    esac; \
    g++ -std=c++20 -O3 -DNDEBUG normalize.cpp -o /normalize; \
    g++ -std=c++20 -O3 -DNDEBUG $cpu $pgo -pthread -c native.cpp -o native.o; \
    g++ -pthread native.o -Wl,-l:liblz4.so.1 -o /native_pipeline

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=build /normalize /native_pipeline /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["/app/run.sh"]
