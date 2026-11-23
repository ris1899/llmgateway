# Running on Colima's k3s

1. Start Colima with Kubernetes enabled (uses the docker runtime, so images
   built with `docker build` land in the same store k3s pulls from):

   ```
   colima start --kubernetes
   ```

2. Build the two images k3s needs (`imagePullPolicy: IfNotPresent` means it
   will use these local images instead of trying to pull from a registry):

   ```
   docker build -f deploy/docker/Dockerfile --build-arg CMD=gateway -t llmgateway:latest .
   docker build -f deploy/docker/Dockerfile --build-arg CMD=mockprovider -t llmgateway-mock:latest .
   ```

3. Apply the manifests:

   ```
   kubectl apply -k deploy/k8s
   ```

4. Reach the gateway from your machine:

   ```
   kubectl -n llmgateway port-forward svc/gateway 8080:80
   ```

Note: this manifest set is intentionally minimal - no Postgres/Redpanda, so
`auth.enabled` and the usage pipeline are off in `configmap.yaml`. Use the
docker compose stack (`deploy/compose.yaml`) for the full-feature setup.
