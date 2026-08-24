FROM docker.io/airbyte/python-connector-base:4.0.2@sha256:9fdb1888c4264cf6fee473649ecb593f56f58e5d0096a87ee0b231777e2e3e73

COPY . /airbyte/integration_code
# README.md is a symlink to a shared doc outside the build context; replace it
# so poetry metadata preparation can read it.
RUN rm -f /airbyte/integration_code/README.md \
    && echo "Source Shopify connector (Fleek fork)" > /airbyte/integration_code/README.md \
    && pip install --no-cache-dir /airbyte/integration_code

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
