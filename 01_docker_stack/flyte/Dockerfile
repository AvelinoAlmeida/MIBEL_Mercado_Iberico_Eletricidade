FROM ghcr.io/flyteorg/flyte-sandbox:latest

# Use the Helm charts bundled inside the image instead of fetching from the
# Helm repo. The image bakes in FLYTE_TEST=release + a specific chart version
# that may no longer exist in the remote index — unsetting FLYTE_TEST causes
# the entrypoint to fall through to the local /flyteorg/share/flyte charts.
ENV FLYTE_TEST=bundled

# Increase Helm install timeout from the default 5 min to 30 min.
# Docker Desktop on Windows/Mac allocates fewer resources than a bare-metal Linux host,
# so the flyte-core Helm chart frequently times out at the default value.
RUN grep -rl -- "--timeout" /usr/local/bin/ 2>/dev/null \
    | xargs sed -i 's/--timeout [0-9]*m[0-9]*s/--timeout 30m0s/g' \
    || true

# Replace Chart.yaml with a stripped version that removes all external
# dependencies whose Helm repos are dead (kubernetes/dashboard → 404,
# spark-on-k8s-operator → 404) or no longer carry the pinned version
# (bitnami removed contour). All three are non-essential for Flyte workflow
# execution; we disable them via --set at helm upgrade time (see below).
# Only flyte-core (file://, local) is kept as a dependency.
COPY flyte-Chart-coreonly.yaml /flyteorg/share/flyte/Chart.yaml

# Vendor the local flyte-core dependency so helm upgrade can run offline.
RUN helm dep update /flyteorg/share/flyte

# Patch the entrypoint to pass --set flags that disable contour,
# kubernetes-dashboard and sparkoperator when calling helm upgrade.
# This avoids any internet access for missing chart tarballs at startup.
RUN sed -i \
    's|helm upgrade -n flyte|helm upgrade -n flyte --set contour.enabled=false --set kubernetes-dashboard.enabled=false --set sparkoperator.enabled=false|' \
    /flyteorg/bin/flyte-entrypoint.sh

# Helm value overrides: point Flyte artifact storage at the Compose MinIO
# instead of the sandbox's internal MinIO.
COPY flyte-core-overrides.yaml /etc/flyte/state/flyte-core-overrides.yaml
