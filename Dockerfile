# syntax=docker/dockerfile:1

# ---- Stage 1: build ---------------------------------------------------
# Compiles TypeScript with full devDependencies available, then discarded.
# Keeping this separate from the runtime stage is what lets the final image
# ship without tsc, ts-node-dev, or @types/* at all.
FROM node:20-alpine AS build
WORKDIR /usr/src/app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- Stage 2: production runtime --------------------------------------
FROM node:20-alpine AS runtime
WORKDIR /usr/src/app
ENV NODE_ENV=production

# Runtime-only dependencies (no TypeScript compiler, no dev tooling) —
# meaningfully shrinks the image and reduces attack surface.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=build /usr/src/app/dist ./dist
COPY public ./public

# Run as a non-root user — a container escape or dependency RCE inside this
# process should not hand an attacker root inside the container.
RUN addgroup -S taskworker && adduser -S taskworker -G taskworker
USER taskworker

EXPOSE 5000

# Lets `docker compose ps` and orchestrators reflect real app health (the
# /metrics endpoint is a lightweight, dependency-free liveness signal since
# it doesn't touch Redis) rather than only "process is running".
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://127.0.0.1:5000/metrics', r => process.exit(r.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1))"

CMD ["node", "dist/server.js"]
