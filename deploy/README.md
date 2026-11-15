# Running sparqix on Kubernetes

The image is stdlib-only Python (no third-party packages), runs as UID 10001 with a read-only root filesystem, all capabilities dropped,
and listens on 8765. The web UI refuses a non-loopback address without a token, so the manifests give it one from a Secret.

## 1. Build and push

    docker build -t ghcr.io/shipitamit/sparqix:0.1.0 .
    docker push ghcr.io/shipitamit/sparqix:0.1.0

Replace `ghcr.io/shipitamit/sparqix` in `deploy/k8s/deployment.yaml` (and `kustomization.yaml`); pin a digest for production.

Pushing a `v*` tag publishes both `ghcr.io/shipitamit/sparqix:v0.1.0` and `:0.1.0` (the manifests use the tag without the `v`), signed by digest.
Change `shipitamit` if you fork the repository. A package on ghcr.io is **private by default**: either make it public (package settings), or create a pull secret and
uncomment `imagePullSecrets` in `deployment.yaml`:

    kubectl -n sparqix create secret docker-registry ghcr-pull --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token with read:packages>

## 2. Deploy

    kubectl apply -f deploy/k8s/namespace.yaml
    kubectl -n sparqix create secret generic sparqix --from-literal=token="$(openssl rand -base64 32)"
    # optional AI suggestions: add --from-literal=ai-api-key=<your provider key> (and set SPARQIX_AI_PROVIDER / SPARQIX_AI_MODEL / SPARQIX_AI_BASE_URL on the Deployment)
    # edit SPARQIX_ALLOWED_HOSTS in deployment.yaml to include your ingress host name
    kubectl apply -k deploy/k8s
    kubectl apply -f deploy/k8s/ingress.example.yaml        # after editing the host; TLS is required

Open `https://<your-host>/?token=<token>`. The page removes the token from the address bar and sends it as a header afterwards.
Quick local check without ingress: `kubectl -n sparqix port-forward svc/sparqix 8765:80`, then open `http://localhost:8765/?token=...`
(port-forwarding keeps the Host header `localhost:8765`, which is always accepted).

## What the manifests set up
- Deployment (2 replicas, rolling update, spread across nodes), Service (ClusterIP), PodDisruptionBudget, HPA (CPU 70%, 2-6 pods).
- Pod Security `restricted`, non-root, read-only root filesystem (a small `emptyDir` at `/tmp` for the per-request worker processes), no service-account token.
- NetworkPolicy: ingress only from the `ingress-nginx` namespace (change the label for your controller); egress only DNS. Outbound HTTPS is **off** by default: apply
  `networkpolicy-ai-egress.example.yaml` as well if you use the AI features.
- Limits: `SPARQIX_MAX_CONCURRENT` (3), `SPARQIX_WORKER_MEMORY_MB` (768) and `SPARQIX_CONVERT_TIMEOUT` (60 s) bound each pod. Keep the container memory limit above
  `MAX_CONCURRENT x WORKER_MEMORY_MB + 300 MB`, otherwise a burst of heavy requests can get the pod OOM-killed.
- Shutdown: on SIGTERM the app stops accepting connections and lets running conversions finish for up to `SPARQIX_DRAIN_SECONDS` (150); a 5 s `preStop` sleep gives the
  ingress time to drop the pod, and `terminationGracePeriodSeconds` (170) stays above both.
- The ingress example sets a 180 s read timeout (the AI assistant can take 150 s) and per-client rate limits.
- Probes use `/healthz` and `/readyz`, which return `ok` without touching any data or requiring the token.
- `SPARQIX_ALLOWED_HOSTS` is the DNS-rebinding guard: a request whose Host header is not listed gets 403.

## Batch conversion
`deploy/k8s/job.example.yaml` runs `sparqix project /in -o /out` as a Job against two PVCs. The CLI never executes the input.
`sparqix verify` is deliberately not part of the image's default flow: it executes code and needs `--trust` (see SECURITY.md).

## Notes
- Sessions are stateless; any replica can serve any request, so no sticky sessions or storage are needed.
- Behind an ingress set a body limit of at least 30 MB (project uploads); the app enforces its own limits as well.
- Plain HTTP carries the token: only expose the Service through TLS.
- Not tested in a real cluster or with a real image build (no Docker daemon or cluster here): the manifests are syntactically valid YAML and the same command line was exercised locally (probes, Host guard, token, convert).
